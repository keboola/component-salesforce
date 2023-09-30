import base64
import logging
from collections import OrderedDict
from enum import Enum
from json import JSONDecodeError
from typing import Iterable

import requests
from keboola.http_client import HttpClient
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.salesforce_bulk import DEFAULT_API_VERSION
from simple_salesforce import Salesforce

NON_SUPPORTED_BULK_FIELD_TYPES = ["address", "location", "base64", "reference"]

# Some objects are not supported by bulk and there is no exact way to determine them, they must be set like this
# https://help.salesforce.com/s/articleView?id=000383508&type=1
OBJECTS_NOT_SUPPORTED_BY_BULK = ["AccountFeed", "AssetFeed", "AccountHistory", "AcceptedEventRelation",
                                 "DeclinedEventRelation", "AggregateResult", "AttachedContentDocument", "CaseStatus",
                                 "CaseTeamMember", "CaseTeamRole", "CaseTeamTemplate", "CaseTeamTemplateMember",
                                 "CaseTeamTemplateRecord", "CombinedAttachment", "ContentFolderItem", "ContractStatus",
                                 "EventWhoRelation", "FolderedContentDocument", "KnowledgeArticleViewStat",
                                 "KnowledgeArticleVoteStat", "LookedUpFromActivity", "Name", "NoteAndAttachment",
                                 "OpenActivity", "OwnedContentDocument", "PartnerRole", "RecentlyViewed",
                                 "ServiceAppointmentStatus", "SolutionStatus", "TaskPriority", "TaskStatus",
                                 "TaskWhoRelation", "UserRecordAccess", "WorkOrderLineItemStatus", "WorkOrderStatus"]


def _backoff_handler(details):
    # this should never happen, but if it does retry login
    if 'InvalidSessionId' in str(details['exception']):
        logging.warning('SessionID invalid, trying to re-login.')
        details['args'][0].relogin()


class Operation(str, Enum):
    insert = "insert"
    upsert = "upsert"
    update = "update"
    delete = "delete"
    hard_delete = "hardDelete"
    query = "query"
    query_all = "queryAll"


class ColumnDelimiter(str, Enum):
    BACKQUOTE = "BACKQUOTE"  # (`)
    CARET = "CARET"  # (^)
    COMMA = "COMMA"  # (,)
    PIPE = "PIPE"  # (|)
    SEMICOLON = "SEMICOLON"  # (;)
    TAB = "TAB"  # (\t)


_delimiter_char = {
    ColumnDelimiter.BACKQUOTE: "`",
    ColumnDelimiter.CARET: "^",
    ColumnDelimiter.COMMA: ",",
    ColumnDelimiter.PIPE: "|",
    ColumnDelimiter.SEMICOLON: ";",
    ColumnDelimiter.TAB: "\t",
}


class LineEnding(str, Enum):
    LF = "LF"
    CRLF = "CRLF"


_line_ending_char = {LineEnding.LF: "\n", LineEnding.CRLF: "\r\n"}


class ResultsType(str, Enum):
    failed = "failedResults"
    successful = "successfulResults"
    unprocessed = "unprocessedRecords"


class SalesforceAuthenticationFailed(Exception):
    pass


class SalesforceClient(HttpClient):
    def __init__(self, consumer_key: str, consumer_secret: str, refresh_token: str, is_sandbox: bool = False,
                 api_version=DEFAULT_API_VERSION, legacy_credentials: dict = None):

        super().__init__('NONE', max_retries=3)

        self._legacy_credentials = legacy_credentials
        self.is_sandbox = is_sandbox
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._refresh_token = refresh_token
        self.is_logged_in = False
        self.api_version = api_version
        self.simple_client: Salesforce

    def login(self):
        if not self.is_sandbox:
            domain = 'login'
        else:
            domain = 'test'

        if self._legacy_credentials:
            access_token, instance_url = SalesforceBulk.login_to_salesforce(self._legacy_credentials['username'],
                                                                            self._legacy_credentials['password'],
                                                                            sandbox=self.is_sandbox,
                                                                            security_token=self._legacy_credentials[
                                                                                'security_token'],
                                                                            API_version=self.api_version)

        else:
            token_data = {'grant_type': 'refresh_token',
                          'refresh_token': self._refresh_token}
            authorization = f'{self._consumer_key}:{self._consumer_secret}'
            encoded = base64.b64encode(authorization.encode()).decode()
            headers = {
                'Authorization': f'Basic {encoded}'
            }

            token_url = f'https://{domain}.salesforce.com/services/oauth2/token'
            response = requests.post(token_url, token_data, headers=headers)

            try:
                json_response = response.json()
            except JSONDecodeError as exc:
                raise SalesforceAuthenticationFailed(
                    response.status_code, response.text
                ) from exc

            if response.status_code != 200:
                except_code = json_response.get('error')
                except_msg = json_response.get('error_description')
                if except_msg == "user hasn't approved this consumer":
                    auth_url = f'https://{domain}.salesforce.com/services/oauth2/' \
                               'authorize?response_type=code&client_id=' \
                               f'{self._consumer_key}&redirect_uri=<approved URI>'
                    except_msg += f"""\n
                  If your connected app policy is set to "All users may
                  self-authorize", you may need to authorize this
                  application first. Browse to
                  {auth_url}
                  in order to Allow Access. Check first to ensure you have a valid
                  <approved URI>."""
                raise SalesforceAuthenticationFailed(except_code, except_msg)

            access_token = json_response.get('access_token')
            instance_url = json_response.get('instance_url')

        sf_instance = instance_url.replace(
            'http://', '').replace(
            'https://', '')

        self.is_logged_in = True
        if sf_instance[0:4] == 'http':
            self.base_url = sf_instance
        else:
            self.base_url = "https://" + sf_instance

        self.update_auth_header({"Authorization": f"Bearer {access_token}"})

        # init simple client
        self._init_simple_client(access_token, domain)

    def _init_simple_client(self, access_token: str, domain: str):
        instance_url = self.base_url
        self.simple_client = Salesforce(session_id=access_token, instance_url=instance_url,
                                        domain=domain, version=self.api_version)

    def create_job_and_upload_data(self, sf_object: str,
                                   operation: Operation,
                                   input_stream: Iterable,
                                   column_delimiter=ColumnDelimiter.COMMA,
                                   line_ending=LineEnding.LF,
                                   external_id_field=None,
                                   assignment_rule_id=None) -> dict:
        """
        Creates job, uploads data and marks upload job as uploadComplete to start processing on salesforce side.
        Returns resulting job
        Args:
            sf_object:
            operation:
            input_stream:
            column_delimiter:
            line_ending:
            external_id_field:
            assignment_rule_id:

        Returns: dict: job object with current status

        """
        job = self.create_upload_job(sf_object, operation,
                                     column_delimiter,
                                     line_ending,
                                     external_id_field,
                                     assignment_rule_id)
        self.upload_data(job['contentUrl'], input_stream)
        self.mark_upload_job_complete(job_id=job['id'])
        return self.get_job_status(job['id'])

    def create_upload_job(
            self,
            sf_object: str,
            operation: Operation,
            column_delimiter=ColumnDelimiter.COMMA,
            line_ending=LineEnding.LF,
            external_id_field=None,
            assignment_rule_id=None
    ):
        payload = {
            "operation": operation,
            "columnDelimiter": column_delimiter,
            "lineEnding": line_ending,
        }
        if external_id_field:
            payload["externalIdFieldName"] = external_id_field

        if assignment_rule_id:
            payload["assignment_rule_id"] = assignment_rule_id

        payload["object"] = sf_object
        payload["contentType"] = "CSV"

        endpoint = f"/services/data/v{self.api_version}/jobs/ingest"
        result = self.post_raw(
            endpoint_path=endpoint,
            json=payload,
        )
        return result.json(object_pairs_hook=OrderedDict)

    def upload_data(self, content_url: str, input_stream: Iterable):

        headers = {'Content-Type': 'text/csv'}

        self.put_raw(endpoint_path=content_url, headers=headers, data=input_stream)

    def mark_upload_job_complete(self, job_id: str):
        endpoint = f'/services/data/v{self.api_version}/jobs/ingest/{job_id}'
        self.patch(endpoint, json={"state": "UploadComplete"})

    def get_job_status(self, job_id: str):
        endpoint = f'/services/data/v{self.api_version}/jobs/ingest/{job_id}'
        return self.get(endpoint)

    def download_failed_results(self, job_id: str, result_path: str):
        endpoint = f'/services/data/v{self.api_version}/jobs/ingest/{job_id}/failedResults'
        res = self.get_raw(endpoint, stream=True)

        with open(result_path, 'wb+') as out:
            for chunk in res.iter_content(chunk_size=8192):
                out.write(chunk)

    def is_job_done(self, job: dict):
        OUTCOME_STATES = ['JobComplete', 'Failed', 'Aborted']
        return job['state'] in OUTCOME_STATES

    def get_bulk_fetchable_objects(self):
        all_s_objects = self.simple_client.describe()["sobjects"]
        to_fetch = []
        # Only objects with the 'queryable' set to True and ones that are not in the OBJECTS_NOT_SUPPORTED_BY_BULK are
        # queryable by the Bulk API. This list might not be exact, and some edge-cases might have to be addressed.
        for sf_object in all_s_objects:
            if sf_object.get('queryable') and not sf_object.get('name') in OBJECTS_NOT_SUPPORTED_BY_BULK:
                to_fetch.append({"label": sf_object.get('label'), 'value': sf_object.get('name')})
        return to_fetch
