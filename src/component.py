import csv
import json
import logging
import os
import shutil
import subprocess
from enum import Enum
from math import floor, ceil
from time import sleep
from typing import Dict, List, Iterator

import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from retry import retry
from salesforce_bulk import CsvDictsAdapter
from simple_salesforce.exceptions import SalesforceAuthenticationFailed

from salesforce.client import SalesforceClient, LineEnding

from buffer_management import DataChunkBufferManager, DataChunkBuffer

KEY_ADVANCED_OPTIONS = 'advanced_options'

KEY_USERNAME = "username"
KEY_PASSWORD = "#password"
KEY_SECURITY_TOKEN = "#security_token"

KEY_LOGIN_METHOD = "login_method"
KEY_CONSUMER_KEY = "consumer_key"
KEY_CONSUMER_SECRET = "#consumer_secret"
KEY_DOMAIN = "domain"
KEY_API_VERSION = "api_version"
KEY_SANDBOX = "sandbox"

KEY_OBJECT = "sf_object"
KEY_REPLACE_STRING = "replace_string"
KEY_OPERATION = "operation"
KEY_ASSIGNMENT_ID = "assignment_id"
KEY_UPSERT_FIELD_NAME = "upsert_field_name"
KEY_SERIAL_MODE = "serial_mode"
KEY_OVERRIDE_BATCH_SIZE = "override_batch_size"
KEY_BATCH_SIZE = "batch_size"
KEY_FAIL_ON_ERROR = "fail_on_error"
KEY_PRINT_FAILED_TO_LOG = "print_failed_to_log"

KEY_PROXY = "proxy"
KEY_USE_PROXY = "use_proxy"
KEY_PROXY_SERVER = "proxy_server"
KEY_PROXY_PORT = "proxy_port"
KEY_PROXY_USERNAME = "username"
KEY_PROXY_PASSWORD = "#password"
KEY_USE_HTTP_PROXY_AS_HTTPS = "use_http_proxy_as_https"

REQUIRED_PARAMETERS = [KEY_OBJECT, KEY_OPERATION]
REQUIRED_IMAGE_PARS = []

LOG_LIMIT = 15
MAX_INGEST_JOB_FILE_SIZE = 100 * 1024 * 1024
MAX_SERIAL_BATCH_SIZE = 2500

DEFAULT_API_VERSION = "40.0"


class LoginType(str, Enum):
    SECURITY_TOKEN_LOGIN = "security_token"
    CONNECTED_APP_OAUTH_CC = "connected_app_oauth_cc"
    CONNECTED_APP_OAUTH_AUTH_CODE = "connected_app_oauth_auth_code"

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


def estimate_chunk_size(csv_path: str) -> int:
    csv_data_size = os.path.getsize(csv_path)
    max_bytes = min(
        csv_data_size, MAX_INGEST_JOB_FILE_SIZE - 1 * 1024 * 1024
    )  # -1 MB for sentinel
    num_lines = get_file_row_count(csv_path)
    if max_bytes == csv_data_size:
        return num_lines
    else:
        return floor(num_lines / ceil(csv_data_size / max_bytes))


def get_file_row_count(input_table_path):
    row_count = int(subprocess.check_output(f"wc -l '{input_table_path}'", shell=True).split()[0])
    return row_count


def skip_first_line(file_path: str) -> list[str]:
    with open(file_path, 'r') as source_file:
        header = next(csv.reader(source_file))  # skip the first line
        with open(file_path + '.mod.csv', 'w') as target_file:
            shutil.copyfileobj(source_file, target_file)
    # remove original
    os.remove(file_path)
    os.rename(file_path + '.mod.csv', file_path)
    return header


def write_table_manifest(table: TableDefinition):
    manifest = table.get_manifest_dictionary()
    # TODO why it is not in the library?
    manifest['incremental'] = False
    if 'queuev2' in os.environ.get('KBC_PROJECT_FEATURE_GATES', ''):
        manifest['write_always'] = True
    else:
        logging.warning("Running on old queue, "
                        "result log will not be stored unless continue on failure is selected")
    with open(table.full_path + '.manifest', 'w') as manifest_file:
        json.dump(manifest, manifest_file)


def get_result_table_name(operation, sf_object):
    config_row_id = os.environ.get("KBC_CONFIGROWID", "KBC_CONFIGROWID")
    result_table_name = f"{sf_object}_{operation}_result_{config_row_id}.csv"
    return result_table_name


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.print_failed_to_log = False
        params = self.configuration.parameters
        login_method = self._get_login_method()
        if login_method == LoginType.CONNECTED_APP_OAUTH_AUTH_CODE:
            # oauth login
            self.client = SalesforceClient(consumer_key=self.configuration.oauth_credentials.appKey,
                                           consumer_secret=self.configuration.oauth_credentials.appSecret,
                                           refresh_token=self.configuration.oauth_credentials.data['refresh_token'],
                                           api_version=params.get(KEY_API_VERSION, DEFAULT_API_VERSION),
                                           is_sandbox=params.get(KEY_SANDBOX))
        elif login_method == LoginType.SECURITY_TOKEN_LOGIN:

            self.validate_configuration_parameters([KEY_USERNAME, KEY_PASSWORD, KEY_SECURITY_TOKEN])
            self.client = SalesforceClient(legacy_credentials=dict(username=params.get(KEY_USERNAME),
                                                                   password=params.get(KEY_PASSWORD),
                                                                   security_token=params.get(KEY_SECURITY_TOKEN)),
                                           api_version=params.get(KEY_API_VERSION, DEFAULT_API_VERSION),
                                           is_sandbox=params.get(KEY_SANDBOX))

        elif login_method == LoginType.CONNECTED_APP_OAUTH_CC:
            self.validate_configuration_parameters([KEY_CONSUMER_KEY, KEY_CONSUMER_SECRET, KEY_DOMAIN])
            self.client = SalesforceClient(consumer_key=params[KEY_CONSUMER_KEY],
                                           consumer_secret=params[KEY_CONSUMER_SECRET],
                                           api_version=params.get(KEY_API_VERSION, DEFAULT_API_VERSION),
                                           is_sandbox=params.get(KEY_SANDBOX),
                                           domain=params[KEY_DOMAIN])

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        params = self.configuration.parameters

        input_table = self.get_input_table()

        if not input_table:
            return

        self.print_failed_to_log = params.get(KEY_PRINT_FAILED_TO_LOG, False)

        sf_object = params.get(KEY_OBJECT)
        operation = params.get(KEY_OPERATION).lower()

        upsert_field_name = params.get(KEY_UPSERT_FIELD_NAME)
        if upsert_field_name:
            upsert_field_name = upsert_field_name.strip()

        assignment_id = params.get(KEY_ASSIGNMENT_ID)
        if assignment_id:
            assignment_id = assignment_id.strip()

        logging.info(f"Running {operation} operation with input table to the {sf_object} Salesforce object")

        serial_mode = params.get(KEY_ADVANCED_OPTIONS, {}).get(KEY_SERIAL_MODE, False)

        replace_string = params.get(KEY_REPLACE_STRING)
        input_headers = input_table.columns
        if replace_string:
            input_table.columns = self.replace_headers(input_headers, replace_string)

        if upsert_field_name and upsert_field_name.strip() not in input_headers:
            raise UserException(
                f"Upsert field name {upsert_field_name} not in input table with headers {input_headers}")

        if operation == "delete" and len(input_headers) != 1:
            raise UserException("Delete operation should only have one column with id, input table contains "
                                f"{len(input_headers)} columns")

        result_table = self.create_result_table(input_table.columns, operation, sf_object)
        buffer_manager = DataChunkBufferManager(self.data_folder_path, result_table, serial_mode)

        run_error: Exception = None
        try:
            self.write_to_salesforce(input_table, upsert_field_name,
                                     sf_object, operation, assignment_id, serial_mode, buffer_manager)
        except SalesforceAuthenticationFailed as e:
            run_error = UserException(f"Authentication Failed: recheck your username, password, and security token. "
                                      f"Exception detail: {e}")
        except Exception as ex:
            run_error = ex

        if buffer_manager.total_unprocessed_buffers() > 0:
            self.write_unprocessed_buffers(buffer_manager, str(run_error))
            logging.warning(f"{buffer_manager.total_unprocessed_buffers()} "
                            f"buffers were not processed will be written to the result table with error message")

        logging.info(f"{operation}ed {buffer_manager.total_success()} records,"
                     f" {buffer_manager.total_error()} errors occurred,"
                     f" more details in {buffer_manager.result_table.full_path}")

        if run_error:
            raise UserException(run_error)
        elif buffer_manager.total_error() > 0:
            raise UserException("Process was unsuccessful due to errors in the result table. Check the result table.")
        else:
            logging.info("Process was successful.")

    def _get_login_method(self) -> LoginType:
        if self.configuration.oauth_credentials:

            login_type_name = 'connected_app_oauth_auth_code'
        else:
            login_type_name = self.configuration.parameters.get(KEY_LOGIN_METHOD, 'security_token')

        try:
            return LoginType(login_type_name)
        except ValueError as val_err:
            raise UserException(
                f"'{login_type_name}' is not a valid Login Type. Enter one of : {LoginType.list()}") from val_err

    @retry(SalesforceAuthenticationFailed, tries=3, delay=10)
    def login_to_salesforce(self):
        self.set_proxy()
        try:
            self.client.login()
        except requests.exceptions.ProxyError as e:
            raise UserException(f"Cannot connect to proxy: {e}")

    def get_input_table(self):
        input_tables = self.get_input_tables_definitions()
        if len(input_tables) == 0:
            raise UserException("No input table added. Please add an input table")
        elif len(input_tables) > 1:
            raise UserException("Too many input tables added. Please add only one input table")
        if get_file_row_count(input_tables[0].full_path) < 2:
            logging.warning("Input table is empty, no data to process")
            return None
        return input_tables[0]

    @staticmethod
    def replace_headers(input_headers, replace_string):
        input_headers = [header.replace(replace_string, ".") for header in input_headers]
        return input_headers

    @staticmethod
    def get_input_file_data(input_table: TableDefinition) -> Iterator[dict]:
        with open(input_table.full_path, mode='r') as in_file:
            reader = csv.DictReader(in_file, fieldnames=input_table.columns)
            # if exists manifest, skip first row
            if input_table.get_manifest_dictionary():
                next(reader)
            for input_row in reader:
                yield input_row

    @staticmethod
    def create_buffers(generator, chunk_size, buffer_manager):
        chunk = []
        for item in generator:
            if len(chunk) >= chunk_size:
                buffer_manager.create_buffer(chunk)
                chunk = [item]
            else:
                chunk.append(item)
        if chunk:
            buffer_manager.create_buffer(chunk)
        logging.debug(
            f"Input data ({buffer_manager.total_rows()} rows) split to {len(buffer_manager.buffers)} chunks")

    def write_to_salesforce(self, input_table: TableDefinition, upsert_field_name,
                            sf_object, operation, assignment_id, serial_mode, buffer_manager):

        input_file_reader = self.get_input_file_data(input_table)
        chunk_size = self.define_chunk_size(input_table, serial_mode)

        self.create_buffers(input_file_reader, chunk_size, buffer_manager)

        # Login needs to be here for possibility write all unprocessed buffers
        self.login_to_salesforce()

        if serial_mode:
            logging.info("Running in serial mode (fall back to Bulk API v1)")
            return self.upload_data_serial(upsert_field_name, sf_object, operation, assignment_id, buffer_manager)
        else:
            logging.info("Running batches in parallel (Bulk 2.0)")
            return self.upload_data_bulk2(upsert_field_name, sf_object, operation, assignment_id, buffer_manager)

    def define_chunk_size(self, input_table, serial_mode):
        batch_size_override = None
        if self.configuration.parameters.get(KEY_ADVANCED_OPTIONS, {}).get(KEY_OVERRIDE_BATCH_SIZE, None):
            batch_size_override = self.configuration.parameters.get(KEY_ADVANCED_OPTIONS, {}).get(KEY_BATCH_SIZE, None)
        if serial_mode:
            chunk_size = batch_size_override or MAX_SERIAL_BATCH_SIZE
        else:
            chunk_size = batch_size_override or estimate_chunk_size(input_table.full_path)

        logging.info(f'Batch size set to: {chunk_size:,}')
        return chunk_size

    def upload_data_bulk2(self, upsert_field_name, sf_object, operation, assignment_id,
                          buffer_manager: DataChunkBufferManager):
        for buffer in buffer_manager.buffers:
            chunk = buffer.get_buffer_data()
            csv_iter = CsvDictsAdapter(iter(chunk))
            logging.info(f"Creating job and uploading buffer #{buffer.id}")
            upload_job = self.client.create_job_and_upload_data(sf_object, operation,
                                                                external_id_field=upsert_field_name,
                                                                line_ending=LineEnding.CRLF,
                                                                assignment_rule_id=assignment_id,
                                                                input_stream=csv_iter)
            buffer.add_job(upload_job.get('id'))
        job_count = len(buffer_manager.buffers)
        logging.info(f"Created and uploaded {job_count} jobs, waiting for the finish")
        finished_jobs = 0
        while job_count > finished_jobs:
            for buffer in buffer_manager.unfinished_jobs():
                sleep(2)
                actual_job = self.client.get_job_status(buffer.job_id)
                if self.client.is_job_done(actual_job):
                    buffer.finish_job(actual_job)
                    self.process_buffer(buffer)
                    finished_jobs = buffer_manager.finished_jobs()
                    logging.info(f'{finished_jobs} jobs finished out of {job_count}')

    def upload_data_serial(self, upsert_field_name, sf_object, operation, assignment_id,
                           buffer_manager: DataChunkBufferManager):

        job_id = self.client.create_job_v1(sf_object, operation, external_id_name=upsert_field_name,
                                           contentType='CSV', concurrency='Serial',
                                           assignement_id=assignment_id)
        logging.info(f"Created job {job_id}")
        for buffer in buffer_manager.get_buffers():
            logging.info(f"Uploading buffer and processing job #{buffer.id}")
            buffer.add_job(job_id)
            chunk = buffer.get_buffer_data()
            csv_iter = CsvDictsAdapter(iter(chunk))
            result = self.client.get_batch_result_v1(job_id, csv_iter)
            buffer.finish_job(result)
            self.process_buffer(buffer)
            logging.info(f"Processing response for #{job_id}")
        self.client.close_job_v1(job_id)
        logging.info(f"Job #{job_id} done")

    def process_buffer(self, buffer):
        if buffer.serial_mode:
            self.parse_result_v1(buffer)
            self.write_result_v1(buffer)
        else:
            self.parse_result_v2(buffer)
            if buffer.job_error_message:
                Component.write_buffer(buffer)
            else:
                self.write_result_v2(buffer)

    @staticmethod
    def parse_result_v1(buffer: DataChunkBuffer):
        num_errors = 0
        num_success = 0
        for chunk in buffer.result:
            if chunk.success == "false":
                num_errors = num_errors + 1
            else:
                num_success = num_success + 1
        buffer.success = num_success
        buffer.error = num_errors

    @staticmethod
    def parse_result_v2(buffer: DataChunkBuffer):
        num_errors = 0
        num_success = 0
        if buffer.result['state'] == 'Failed':
            num_errors = buffer.row_count
            buffer.job_error_message = buffer.result['errorMessage']
            logging.debug(f"Job {buffer.job_id} failed with message: {buffer.job_error_message}")
        else:
            num_errors = num_errors + buffer.result['numberRecordsFailed']
            num_success = num_success + (buffer.result['numberRecordsProcessed'] - buffer.result['numberRecordsFailed'])
        buffer.success = num_success
        buffer.error = num_errors

    def write_result_v2(self, buffer: DataChunkBuffer):

        result_table = buffer.result_table
        if buffer.success > 0:
            success_result_key = "successfulResults"
            self.process_result_v2(buffer, result_table, success_result_key)
        if buffer.error > 0:
            error_result_key = "failedResults"
            self.process_result_v2(buffer, result_table, error_result_key)
        buffer.process_done()
        # TODO: remove when write_always added to the library
        write_table_manifest(result_table)

    def process_result_v2(self, buffer, result_table, result_key):
        file_path = os.path.join(result_table.full_path, f'{buffer.id}.csv')
        sf_job_result_file_path = os.path.join(result_table.full_path, f'{buffer.id}_{result_key}.csv')
        self.client.download_results(buffer.job_id, sf_job_result_file_path, result_key)
        with open(sf_job_result_file_path, 'r') as result_file:
            result_file_reader = csv.DictReader(result_file)
            with open(file_path, 'a', newline='') as out_table:
                writer = csv.DictWriter(out_table, fieldnames=result_table.columns, lineterminator='\n', delimiter=',')
                writer.writerows(result_file_reader)
        os.remove(sf_job_result_file_path)

    @staticmethod
    def write_result_v1(buffer: DataChunkBuffer):
        result_table = buffer.result_table

        file_path = os.path.join(result_table.full_path, f'{buffer.id}.csv')
        with open(file_path, 'w+', newline='') as out_table:
            writer = csv.DictWriter(out_table, fieldnames=result_table.columns, lineterminator='\n', delimiter=',')
            for i, row in enumerate(buffer.get_buffer_data()):
                if buffer.result[i].success == "true":
                    row["sf__Id"] = buffer.result[i].id
                    row["sf__Created"] = buffer.result[i].created
                    writer.writerow(row)
                else:
                    row["sf__Error"] = buffer.result[i].error
                    writer.writerow(row)
            buffer.process_done()

        # TODO: remove when write_always added to the library
        write_table_manifest(result_table)

    @staticmethod
    def write_unprocessed_buffers(buffer_manager: DataChunkBufferManager, error_message):
        for buffer in buffer_manager.unprocessed_buffers():
            Component.write_buffer(buffer, error_message)

    @staticmethod
    def write_buffer(buffer, error_message=None):
        """
        Writes buffer to the result table with error message
        write_table_manifest is called every time to create a new manifest file in case when new columns are added
        to the result table.
        """
        logging.debug(f"Writing buffer {buffer.id} to the result table")
        result_table = buffer.result_table
        file_path = os.path.join(result_table.full_path, f'{buffer.id}.csv')
        with open(file_path, 'w+', newline='') as out_table:
            writer = csv.DictWriter(out_table, fieldnames=result_table.columns, lineterminator='\n', delimiter=',')
            for i, row in enumerate(buffer.get_buffer_data()):
                row["kbc__Error"] = error_message
                row["sf__Error"] = buffer.job_error_message
                writer.writerow(row)
        buffer.process_done()

        write_table_manifest(result_table)

    def create_result_table(self, columns, operation, sf_object) -> TableDefinition:
        """
        Ensures the result table and its manifest file are created at the beginning of the run.
        write_table_manifest is called every time to create a new manifest file in case when new columns are added
        to the result table.
        """
        fieldnames = ["sf__Id", "sf__Created", "sf__Error", "kbc__Error"]
        fieldnames.extend(columns)
        result_table_name = get_result_table_name(operation, sf_object)
        logging.debug(f"Creating result table {result_table_name} with columns {fieldnames}")
        result_table = self.create_out_table_definition(name=result_table_name)
        result_table.columns = fieldnames
        os.makedirs(result_table.full_path, exist_ok=True)

        write_table_manifest(result_table)
        return result_table

    def set_proxy(self) -> None:
        """Sets proxy if defined"""
        proxy_config = self.configuration.parameters.get(KEY_PROXY, {})
        if proxy_config.get(KEY_USE_PROXY):
            self._set_proxy(proxy_config)

    def _set_proxy(self, proxy_config: dict) -> None:
        """
        Sets proxy using environmental variables.
        Also, a special case when http proxy is used for https is handled by using KEY_USE_HTTP_PROXY_AS_HTTPS.
        os.environ['HTTPS_PROXY'] = (username:password@)your.proxy.server.com(:port)
        """
        proxy_server = proxy_config.get(KEY_PROXY_SERVER)
        proxy_port = str(proxy_config.get(KEY_PROXY_PORT))
        proxy_username = proxy_config.get(KEY_PROXY_USERNAME)
        proxy_password = proxy_config.get(KEY_PROXY_PASSWORD)
        use_http_proxy_as_https = proxy_config.get(
            KEY_USE_HTTP_PROXY_AS_HTTPS) or self.configuration.image_parameters.get(KEY_USE_HTTP_PROXY_AS_HTTPS)

        if not proxy_server:
            raise UserException("You have selected use_proxy parameter, but you have not specified proxy server.")
        if not proxy_port:
            raise UserException("You have selected use_proxy parameter, but you have not specified proxy port.")

        _proxy_credentials = f"{proxy_username}:{proxy_password}@" if proxy_username and proxy_password else ""
        _proxy_server = f"{_proxy_credentials}{proxy_server}:{proxy_port}"

        if use_http_proxy_as_https:
            # This is a case of http proxy which also supports https.
            _proxy_server = f"http://{_proxy_server}"
        else:
            _proxy_server = f"https://{_proxy_server}"

        os.environ["HTTPS_PROXY"] = _proxy_server

        logging.info(f"Component will use proxy server {_proxy_server}.")

    @sync_action('loadObjects')
    def load_possible_objects(self) -> List[Dict]:
        """
        Finds all possible objects in Salesforce that can be fetched by the Bulk API

        Returns: a List of dictionaries containing 'name' and 'value' of the SF object, where 'name' is the Label name/
        readable name of the object, and 'value' is the name of the object you can use to query the object

        """
        self.get_salesforce_client()
        return self.client.get_bulk_fetchable_objects()

    def get_salesforce_client(self) -> SalesforceClient:
        try:
            return self.login_to_salesforce()
        except SalesforceAuthenticationFailed as e:
            raise UserException("Authentication Failed : recheck your username, password, and security token ") from e

    @sync_action('testConnection')
    def test_connection(self):
        """
        Tries to log into Salesforce, raises user exception if login params are incorrect
        """
        self.get_salesforce_client()


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
