'''
Template Component main class.

'''
from retry import retry
import logging
import csv
from simple_salesforce.exceptions import SalesforceAuthenticationFailed
from salesforce_bulk import CsvDictsAdapter

from keboola.component.base import ComponentBase, UserException
from salesforce.client import SalesforceClient

KEY_USERNAME = "username"
KEY_PASSWORD = "#password"
KEY_SECURITY_TOKEN = "#security_token"
KEY_SANDBOX = "sandbox"

KEY_OBJECT = "sf_object"
KEY_REPLACE_STRING = "replace_string"
KEY_OPERATION = "operation"
KEY_ASSIGNMENT_ID = "assignment_id"
KEY_UPSERT_FIELD_NAME = "upsert_field_name"
KEY_SERIAL_MODE = "serial_mode"
KEY_OUTPUT_ERRORS = "output_errors"

REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_OBJECT, KEY_PASSWORD, KEY_SECURITY_TOKEN, KEY_OPERATION]
REQUIRED_IMAGE_PARS = []

BATCH_LIMIT = 8000


class Component(ComponentBase):
    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self):
        '''
        Main execution code
        '''

        params = self.configuration.parameters

        input_table = self.get_input_table()

        try:
            salesforce_client = self.login_to_salesforce(params)
        except SalesforceAuthenticationFailed:
            raise UserException("Authentication Failed : recheck your username, password, and security token ")

        sf_object = params.get(KEY_OBJECT)

        operation = params.get(KEY_OPERATION).lower()
        upsert_field_name = params.get(KEY_UPSERT_FIELD_NAME)
        assignement_id = params.get(KEY_ASSIGNMENT_ID)

        logging.info(f"Running {operation} operation with input table to the {sf_object} Salesforce object")

        if params.get(KEY_SERIAL_MODE):
            concurrency = 'Serial'
        else:
            concurrency = 'Parallel'

        replace_string = params.get(KEY_REPLACE_STRING)
        input_headers = self.get_input_table_headers(input_table.full_path)
        if replace_string:
            input_headers = self.replace_headers(input_headers, replace_string)

        input_file_reader = self.get_input_file_reader(input_table, input_headers)

        if operation == "delete" and input_headers.len() != 1:
            raise UserException("Delete operation should only have one column with id, input table contains "
                                "{input_headers.len()} columns")

        results = self.write_to_salesforce(input_file_reader, upsert_field_name, salesforce_client,
                                           sf_object, operation, concurrency, assignement_id)
        parsed_results, num_success, num_errors = self.parse_results(results)

        logging.info(
            f"All data written to salesforce, {operation}ed {num_success} records, {num_errors} errors occurred")

        self.write_unsuccessful(parsed_results, input_table, input_headers, sf_object, operation)

    @retry(SalesforceAuthenticationFailed, tries=3, delay=5)
    def login_to_salesforce(self, params):
        return SalesforceClient(username=params.get(KEY_USERNAME),
                                password=params.get(KEY_PASSWORD),
                                security_token=params.get(KEY_SECURITY_TOKEN),
                                sandbox=params.get(KEY_SANDBOX))

    def get_input_table(self):
        input_tables = self.get_input_tables_definitions()
        if len(input_tables) == 0:
            raise UserException("No input table added. Please add an input table")
        elif len(input_tables) > 1:
            raise UserException("Too many input tables added. Please add only one input table")
        return input_tables[0]

    @staticmethod
    def get_input_table_headers(table_path):
        with open(table_path, 'r') as f:
            d_reader = csv.DictReader(f)
            return d_reader.fieldnames

    @staticmethod
    def replace_headers(input_headers, replace_string):
        input_headers = [header.replace(replace_string, ".") for header in input_headers]
        return input_headers

    @staticmethod
    def get_input_file_reader(input_table, input_headers):
        with open(input_table.full_path, mode='r') as in_file:
            reader = csv.DictReader(in_file, fieldnames=input_headers)
            #  skip first row headers as input header has been specified
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

    def get_job_result(self, salesforce_client, job, csv_iter):
        batch = salesforce_client.post_batch(job, csv_iter)
        salesforce_client.wait_for_batch(job, batch)
        salesforce_client.close_job(job)
        result = salesforce_client.get_batch_results(batch)
        return result

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

    def write_to_salesforce(self, input_file_reader, upsert_field_name, salesforce_client, sf_object, operation,
                            concurrency, assignement_id):
        results = []
        for i, chunk in enumerate(self.get_chunks(input_file_reader, BATCH_LIMIT)):
            if upsert_field_name:
                job = salesforce_client.create_job(sf_object, operation, external_id_name=upsert_field_name,
                                                   contentType='CSV', concurrency=concurrency,
                                                   assignement_id=assignement_id)
            else:
                job = salesforce_client.create_job(sf_object, operation, contentType='CSV', concurrency=concurrency,
                                                   assignement_id=assignement_id)
            csv_iter = CsvDictsAdapter(iter(chunk))
            job_result = self.get_job_result(salesforce_client, job, csv_iter)
            results.extend(job_result)
        return results

    def write_unsuccessful(self, parsed_results, input_table, input_headers, sf_object, operation):
        unsuccessful_table_name = "".join([sf_object, "_", operation, "_unsuccessful.csv"])
        logging.info(f"Saving errors to {unsuccessful_table_name}")
        fieldnames = input_headers
        fieldnames.append("error")
        unsuccessful_table = self.create_out_table_definition(name=unsuccessful_table_name, columns=fieldnames)
        with open(unsuccessful_table.full_path, 'w+', newline='') as out_table:
            writer = csv.DictWriter(out_table, fieldnames=fieldnames, lineterminator='\n', delimiter=',')
            with open(input_table.full_path, 'r') as input_table:
                reader = csv.DictReader(input_table)
                for i, row in enumerate(reader):
                    if parsed_results[i]["success"] == "false":
                        error_row = row
                        error_row["error"] = parsed_results[i]["error"]
                        writer.writerow(error_row)
        self.write_tabledef_manifest(unsuccessful_table)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
