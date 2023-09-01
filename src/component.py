import csv
import json
import logging
import os
from typing import Dict, List

import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from retry import retry
from salesforce_bulk import CsvDictsAdapter, BulkApiError
from simple_salesforce.exceptions import SalesforceAuthenticationFailed

from salesforce.client import SalesforceClient

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

KEY_PROXY = "proxy"
KEY_USE_PROXY = "use_proxy"
KEY_HTTPS_PROXY = "#https_proxy"

REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_OBJECT, KEY_PASSWORD, KEY_SECURITY_TOKEN, KEY_OPERATION]
REQUIRED_IMAGE_PARS = []

BATCH_LIMIT = 2500
LOG_LIMIT = 15

DEFAULT_API_VERSION = "40.0"


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        params = self.configuration.parameters

        self.set_proxy(params)

        input_table = self.get_input_table()

        try:
            salesforce_client = self.login_to_salesforce(params)
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
            input_headers = self.replace_headers(input_headers, replace_string)
        if upsert_field_name and upsert_field_name.strip() not in input_headers:
            raise UserException(
                f"Upsert field name {upsert_field_name} not in input table with headers {input_headers}")

        input_file_reader = self.get_input_file_reader(input_table, input_headers)

        if operation == "delete" and len(input_headers) != 1:
            raise UserException("Delete operation should only have one column with id, input table contains "
                                f"{len(input_headers)} columns")

        try:
            results = self.write_to_salesforce(input_file_reader, upsert_field_name, salesforce_client,
                                               sf_object, operation, concurrency, assignement_id)
        except BulkApiError as bulk_error:
            raise UserException(bulk_error) from bulk_error

        parsed_results, num_success, num_errors = self.parse_results(results)

        logging.info(
            f"All data written to salesforce, {operation}ed {num_success} records, {num_errors} errors occurred")

        if num_errors > 0:
            self._process_failures(parsed_results, input_headers, sf_object, operation, num_errors)
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
        self.write_unsuccessful(parsed_results, input_headers, sf_object, operation)
        error_table = self.get_error_table_name(operation, sf_object)

        if self.configuration.parameters.get(KEY_FAIL_ON_ERROR):
            raise UserException(
                f"{num_errors} errors occurred. "
                f"Additional details are available in the error log table: {error_table}")
        else:
            logging.warning(f"{num_errors} errors occurred. "
                            "The process is marked as success because the 'Fail on error' parameter is set to false. "
                            f"Additional details are available in the error log table: {error_table}")

    @retry(SalesforceAuthenticationFailed, tries=3, delay=5)
    def login_to_salesforce(self, params):
        try:
            client = SalesforceClient(username=params.get(KEY_USERNAME),
                                      password=params.get(KEY_PASSWORD),
                                      security_token=params.get(KEY_SECURITY_TOKEN),
                                      API_version=params.get(KEY_API_VERSION, DEFAULT_API_VERSION),
                                      sandbox=params.get(KEY_SANDBOX))
        except requests.exceptions.ProxyError as e:
            raise UserException(f"Cannot connect to proxy: {e}")

        return client

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
    def get_input_file_reader(input_table, input_headers):
        with open(input_table.full_path, mode='r') as in_file:
            reader = csv.DictReader(in_file, fieldnames=input_headers)
            for input_row in reader:
                if sorted(input_row.values()) == sorted(input_headers):
                    logging.debug("Skipping header")
                else:
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
    @retry(delay=10, tries=4, backoff=2, exceptions=requests.exceptions.ConnectionError)
    def get_job_result(salesforce_client, job, csv_iter):
        batch = salesforce_client.post_batch(job, csv_iter)
        salesforce_client.wait_for_batch(job, batch)
        salesforce_client.close_job(job)
        return salesforce_client.get_batch_results(batch)

    @staticmethod
    def parse_results(results):
        parsed_results = []
        num_errors = 0
        num_success = 0
        for result in results:
            parsed_results.append({"id": result.id, "success": result.success, "error": result.error})
            if result.success == "false":
                num_errors = num_errors + 1
            else:
                num_success = num_success + 1
        return parsed_results, num_success, num_errors

    def write_to_salesforce(self, input_file_reader, upsert_field_name, salesforce_client,
                            sf_object, operation, concurrency, assignement_id):
        results = []
        for i, chunk in enumerate(self.get_chunks(input_file_reader, BATCH_LIMIT)):
            logging.info(f"Processing chunk #{i}")
            job_result = self.process_job(upsert_field_name, salesforce_client, sf_object, operation, concurrency,
                                          assignement_id, chunk)
            results.extend(job_result)
        return results

    @retry(delay=10, tries=4, backoff=2, exceptions=BulkApiError)
    def process_job(self, upsert_field_name, salesforce_client, sf_object, operation, concurrency, assignement_id,
                    chunk):

        job = salesforce_client.create_job(sf_object, operation, external_id_name=upsert_field_name,
                                           contentType='CSV', concurrency=concurrency,
                                           assignement_id=assignement_id)

        csv_iter = CsvDictsAdapter(iter(chunk))
        return self.get_job_result(salesforce_client, job, csv_iter)

    def write_unsuccessful(self, parsed_results, input_headers, sf_object, operation):
        unsuccessful_table_name = self.get_error_table_name(operation, sf_object)
        logging.info(f"Saving errors to {unsuccessful_table_name}")
        fieldnames = input_headers.copy()
        fieldnames.append("error")
        unsuccessful_table = self.create_out_table_definition(name=unsuccessful_table_name, columns=fieldnames)
        with open(unsuccessful_table.full_path, 'w+', newline='') as out_table:
            writer = csv.DictWriter(out_table, fieldnames=fieldnames, lineterminator='\n', delimiter=',')
            in_file_reader = self.get_input_file_reader(self.get_input_table(), input_headers)
            for i, row in enumerate(in_file_reader):
                if parsed_results[i]["success"] == "false":
                    error_row = row
                    error_row["error"] = parsed_results[i]["error"]
                    writer.writerow(error_row)

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

    def get_error_table_name(self, operation, sf_object):
        unsuccessful_table_name = "".join([sf_object, "_", operation, "_unsuccessful.csv"])
        return unsuccessful_table_name

    def log_errors(self, parsed_results, input_table, input_headers):
        logging.warning(f"Logging first {LOG_LIMIT} errors")
        fieldnames = input_headers.copy()
        fieldnames.append("error")
        for i, row in enumerate(self.get_input_file_reader(input_table, input_headers)):
            if parsed_results[i]["success"] == "false":
                error_row = row
                error_row["error"] = parsed_results[i]["error"]
                logging.warning(f"Failed to update row : {error_row}")
            if i >= LOG_LIMIT - 1:
                break

    def set_proxy(self, params: dict):
        proxy_config = params.get(KEY_PROXY, {})
        if proxy_config.get(KEY_USE_PROXY):
            self._set_proxy(proxy_config)

    @staticmethod
    def _set_proxy(proxy_config: dict) -> None:
        """
        Sets proxy using environmental variables
        os.environ['HTTPS_PROXY'] = 'https://proxy.server:port'
        """
        https_proxy = proxy_config.get(KEY_HTTPS_PROXY)

        if not https_proxy:
            raise UserException("You have selected use_proxy parameter, but you have not configured any proxies.")

        if https_proxy:
            # This is a case of special non-credentials http proxy which also supports https proxy used in CSAS
            os.environ["HTTPS_PROXY"] = f"http://{https_proxy}"
            os.environ["HTTP_PROXY"] = f"http://{https_proxy}"
            logging.info("Component will use proxy.")

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
