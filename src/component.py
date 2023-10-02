import csv
import json
import logging
import os
import shutil
import subprocess
from math import floor
from typing import Dict, List, Iterator

import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from retry import retry
from salesforce_bulk import CsvDictsAdapter, BulkApiError
from simple_salesforce.exceptions import SalesforceAuthenticationFailed

from salesforce.client import SalesforceClient, LineEnding

KEY_USERNAME = "username"
KEY_PASSWORD = "#password"
KEY_SECURITY_TOKEN = "#security_token"
KEY_API_VERSION = "api_version"
KEY_SANDBOX = "sandbox"

KEY_OBJECT = "sf_object"
KEY_REPLACE_STRING = "replace_string"
KEY_OPERATION = "operation"
KEY_ASSIGNMENT_ID = "assignment_id"
KEY_UPSERT_FIELD_NAME = "upsert_field_name"
KEY_SERIAL_MODE = "serial_mode"
KEY_FAIL_ON_ERROR = "fail_on_error"
KEY_PRINT_FAILED_TO_LOG = "print_failed_to_log"

KEY_PROXY = "proxy"
KEY_USE_PROXY = "use_proxy"
KEY_PROXY_SERVER = "proxy_server"
KEY_PROXY_PORT = "proxy_port"
KEY_PROXY_USERNAME = "username"
KEY_PROXY_PASSWORD = "#password"
KEY_USE_HTTP_PROXY_AS_HTTPS = "use_http_proxy_as_https"

REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_OBJECT, KEY_PASSWORD, KEY_SECURITY_TOKEN, KEY_OPERATION]
REQUIRED_IMAGE_PARS = []

LOG_LIMIT = 15
MAX_INGEST_JOB_FILE_SIZE = 100 * 1024 * 1024

DEFAULT_API_VERSION = "40.0"


def estimate_chunk_size(csv_path: str) -> int:
    csv_data_size = os.path.getsize(csv_path)
    max_bytes = min(
        csv_data_size, MAX_INGEST_JOB_FILE_SIZE - 1 * 1024 * 1024
    )  # -1 MB for sentinel
    num_lines = int(subprocess.check_output(f"wc -l {csv_path}", shell=True).split()[0]) - 1
    if max_bytes == csv_data_size:
        return num_lines
    else:
        return floor(num_lines / floor(csv_data_size / max_bytes))


def skip_first_line(file_path: str) -> list[str]:
    with open(file_path, 'r') as source_file:
        header = next(csv.reader(source_file))  # skip the first line
        with open(file_path + '.mod.csv', 'w') as target_file:
            shutil.copyfileobj(source_file, target_file)
    # remove original
    os.remove(file_path)
    os.rename(file_path + '.mod.csv', file_path)
    return header


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.print_failed_to_log = False
        params = self.configuration.parameters
        if self.configuration.oauth_credentials:
            # oauth login
            self.client = SalesforceClient(consumer_key=self.configuration.oauth_credentials.appKey,
                                           consumer_secret=self.configuration.oauth_credentials.appSecret,
                                           refresh_token=self.configuration.oauth_credentials.data['refresh_token'],
                                           api_version=params.get(KEY_API_VERSION, DEFAULT_API_VERSION),
                                           is_sandbox=params.get(KEY_SANDBOX))
        else:
            self.client = SalesforceClient(None, None, None,
                                           legacy_credentials=dict(username=params.get(KEY_USERNAME),
                                                                   password=params.get(KEY_PASSWORD),
                                                                   security_token=params.get(KEY_SECURITY_TOKEN)),
                                           api_version=params.get(KEY_API_VERSION, DEFAULT_API_VERSION),
                                           is_sandbox=params.get(KEY_SANDBOX))

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        params = self.configuration.parameters

        self.set_proxy(params)

        input_table = self.get_input_table()

        self.print_failed_to_log = params.get(KEY_PRINT_FAILED_TO_LOG, False)

        try:
            self.login_to_salesforce()
        except SalesforceAuthenticationFailed as e:
            raise UserException("Authentication Failed : recheck your username, password, and security token ") from e

        sf_object = params.get(KEY_OBJECT)
        operation = params.get(KEY_OPERATION).lower()

        upsert_field_name = params.get(KEY_UPSERT_FIELD_NAME)
        if upsert_field_name:
            upsert_field_name = upsert_field_name.strip()

        assignement_id = params.get(KEY_ASSIGNMENT_ID)
        if assignement_id:
            assignement_id = assignement_id.strip()

        logging.info(f"Running {operation} operation with input table to the {sf_object} Salesforce object")

        concurrency = 'Serial' if params.get(KEY_SERIAL_MODE) else 'Parallel'

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

        try:
            results = self.write_to_salesforce(input_table, upsert_field_name,
                                               sf_object, operation, concurrency, assignement_id)
        except BulkApiError as bulk_error:
            raise UserException(bulk_error) from bulk_error

        # TODO: adjust to new structure
        failed_jobs, num_success, num_errors = self.parse_results(results)

        logging.info(
            f"All data written to salesforce, {operation}ed {num_success} records, {num_errors} errors occurred")

        if num_errors > 0:
            # TODO: adjust to new logic downloading the failed results and adjusting the headers
            # the filed will be sliced.
            self._process_failures(failed_jobs, input_headers, sf_object, operation, num_errors)
        else:
            logging.info("Process was successful")

    def _process_failures(self, parsed_results, input_headers, sf_object, operation, num_errors: int):
        """
        Process and output log of failed records.
        Args:
            parsed_results:
            input_headers:
            sf_object:
            operation:
            num_errors:

        Returns:

        """
        error_table = self.write_unsuccessful(parsed_results, sf_object, operation)

        if self.configuration.parameters.get(KEY_FAIL_ON_ERROR):
            raise UserException(
                f"{num_errors} errors occurred. "
                f"Additional details are available in the error log table: {error_table.name}")
        else:
            logging.warning(f"{num_errors} errors occurred. "
                            "The process is marked as success because the 'Fail on error' parameter is set to false. "
                            f"Additional details are available in the error log table: {error_table.name}")

    @retry(SalesforceAuthenticationFailed, tries=2, delay=5)
    def login_to_salesforce(self):
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
        return input_tables[0]

    @staticmethod
    def replace_headers(input_headers, replace_string):
        input_headers = [header.replace(replace_string, ".") for header in input_headers]
        return input_headers

    @staticmethod
    def get_input_file_data(input_table: TableDefinition) -> Iterator[dict]:
        with open(input_table.full_path, mode='r') as in_file:
            reader = csv.DictReader(in_file, fieldnames=input_table.columns)
            # there will always be input header
            # TODO: this is not true only when before manifests are used, add support
            next(reader)
            for input_row in reader:
                yield input_row

    @staticmethod
    def get_chunks(generator, chunk_size):
        chunk = []
        for item in generator:
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = [item]
            else:
                chunk.append(item)
        if chunk:
            yield chunk

    @staticmethod
    def parse_results(results):
        failed_jobs = []
        num_errors = 0
        num_success = 0
        for result in results:
            num_errors = num_errors + result['numberRecordsFailed']
            num_success = num_success + (result['numberRecordsProcessed'] - result['numberRecordsFailed'])
            if result['state'] in ['Failed', 'Aborted'] or result['numberRecordsFailed'] > 0:
                failed_jobs.append(result)

        return failed_jobs, num_success, num_errors

    def write_to_salesforce(self, input_table: TableDefinition, upsert_field_name,
                            sf_object, operation, concurrency, assignement_id):
        upload_jobs = []
        input_file_reader = self.get_input_file_data(input_table)
        for i, chunk in enumerate(self.get_chunks(input_file_reader,
                                                  estimate_chunk_size(input_table.full_path))):
            logging.info(f"Uploading chunk #{i}")
            upload_job = self.upload_data(upsert_field_name, sf_object, operation, concurrency,
                                          assignement_id, chunk)
            upload_jobs.append(upload_job)
        finished_jobs = []
        job_count = len(upload_jobs)
        logging.info(f"Waiting for {job_count} chunks to finish")
        while job_count > len(finished_jobs):
            for job in upload_jobs:
                actual_job = self.client.get_job_status(job['id'])
                if self.client.is_job_done(actual_job):
                    logging.info(f'{len(finished_jobs)} chunks finished.')
                    finished_jobs.append(actual_job)
        return finished_jobs

    def upload_data(self, upsert_field_name, sf_object, operation, concurrency, assignement_id,
                    chunk):
        csv_iter = CsvDictsAdapter(iter(chunk))
        # TODO: figure out how to use dicts CsvDictsAdapter properly
        job = self.client.create_job_and_upload_data(sf_object, operation, external_id_field=upsert_field_name,
                                                     line_ending=LineEnding.CRLF,
                                                     assignment_rule_id=assignement_id,
                                                     input_stream=csv_iter)
        return job

    def write_unsuccessful(self, failed_jobs, sf_object, operation):

        if not failed_jobs:
            return ''
        unsuccessful_table_name = self.get_error_table_name(operation, sf_object)
        unsuccessful_table = self.create_out_table_definition(name=unsuccessful_table_name)
        os.makedirs(unsuccessful_table.full_path, exist_ok=True)

        # download slices
        header = []

        for i, job in enumerate(failed_jobs):
            err_file_path = os.path.join(unsuccessful_table.full_path, f'{i}.csv')
            self.client.download_failed_results(job['id'], err_file_path)
            header = skip_first_line(err_file_path)

        fieldnames = header
        unsuccessful_table.columns = fieldnames

        # TODO: remove when write_always added to the library
        # self.write_manifest(unsuccessful_table)
        manifest = unsuccessful_table.get_manifest_dictionary()
        if 'queuev2' in os.environ.get('KBC_PROJECT_FEATURE_GATES', ''):
            manifest['write_always'] = True
        else:
            logging.warning("Running on old queue, "
                            "result log will not be stored unless continue on failure is selected")
        with open(unsuccessful_table.full_path + '.manifest', 'w') as manifest_file:
            json.dump(manifest, manifest_file)

        return unsuccessful_table

    def get_error_table_name(self, operation, sf_object):
        unsuccessful_table_name = "".join([sf_object, "_", operation, "_unsuccessful.csv"])
        return unsuccessful_table_name

    def log_errors(self, parsed_results, input_table, input_headers):
        logging.warning(f"Logging first {LOG_LIMIT} errors")
        fieldnames = input_headers.copy()
        fieldnames.append("error")
        for i, row in enumerate(self.get_input_file_data(input_table, input_headers)):
            if parsed_results[i]["success"] == "false":
                error_row = row
                error_row["error"] = parsed_results[i]["error"]
                logging.warning(f"Failed to update row : {error_row}")
            if i >= LOG_LIMIT - 1:
                break

    def set_proxy(self, params: dict) -> None:
        """Sets proxy if defined"""
        proxy_config = params.get(KEY_PROXY, {})
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

        logging.info("Component will use proxy server.")

    @sync_action('loadObjects')
    def load_possible_objects(self) -> List[Dict]:
        """
        Finds all possible objects in Salesforce that can be fetched by the Bulk API

        Returns: a List of dictionaries containing 'name' and 'value' of the SF object, where 'name' is the Label name/
        readable name of the object, and 'value' is the name of the object you can use to query the object

        """
        params = self.configuration.parameters
        salesforce_client = self.get_salesforce_client(params)
        return salesforce_client.get_bulk_fetchable_objects()

    def get_salesforce_client(self, params) -> SalesforceClient:
        try:
            return self.login_to_salesforce(params)
        except SalesforceAuthenticationFailed as e:
            raise UserException("Authentication Failed : recheck your username, password, and security token ") from e

    @sync_action('testConnection')
    def test_connection(self):
        """
        Tries to log into Salesforce, raises user exception if login params are incorrect
        """
        params = self.configuration.parameters
        self.set_proxy(params)
        self.get_salesforce_client(params)


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
