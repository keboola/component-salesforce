import base64
import logging
import xml.etree.ElementTree as ET
from collections import OrderedDict
from json import JSONDecodeError
from typing import Iterable

import backoff
import requests
import salesforce_bulk
from keboola.http_client import HttpClient
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.salesforce_bulk import DEFAULT_API_VERSION, BulkApiError
from simple_salesforce import Salesforce
from simple_salesforce.bulk2 import Operation, ColumnDelimiter, LineEnding
from six import text_type

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


class SalesforceAuthenticationFailed(Exception):
    pass


class SalesforceClient(HttpClient):
    def __init__(self, consumer_key: str, consumer_secret: str, refresh_token: str, is_sandbox: bool = False,
                 api_version=DEFAULT_API_VERSION, legacy_credentials: dict = None, domain=None):

        super().__init__('NONE', max_retries=3)

        self._legacy_credentials = legacy_credentials
        self.is_sandbox = is_sandbox
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._refresh_token = refresh_token
        self.is_logged_in = False
        self.api_version = api_version
        self.simple_client: Salesforce
        self.bulk1_client: salesforce_bulk.SalesforceBulk
        self.domain = domain

    def login(self):
        # present only for client credentials flow
        if not self.domain:
            self.domain = 'login' if not self.is_sandbox else 'test'

        if self._legacy_credentials:

            access_token, instance_url = SalesforceBulk.login_to_salesforce(self._legacy_credentials['username'],
                                                                            self._legacy_credentials['password'],
                                                                            sandbox=self.is_sandbox,
                                                                            security_token=self._legacy_credentials[
                                                                                'security_token'],
                                                                            API_version=self.api_version)

        else:
            instance_url, access_token = self._login_oauth(self.domain)

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
        self._init_simple_client(access_token, self.domain)
        self._init_bulk1_client(access_token, sf_instance)

    def _login_oauth(self, domain: str):
        if self._refresh_token:
            token_data = {'grant_type': 'refresh_token',
                          'refresh_token': self._refresh_token}
            token_url = f'https://{domain}.salesforce.com/services/oauth2/token'

        else:
            token_data = {'grant_type': 'client_credentials'}
            token_url = f'https://{domain}/services/oauth2/token'

        authorization = f'{self._consumer_key}:{self._consumer_secret}'
        encoded = base64.b64encode(authorization.encode()).decode()
        headers = {
            'Authorization': f'Basic {encoded}'
        }
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
        return instance_url, access_token

    def _init_simple_client(self, access_token: str, domain: str):
        instance_url = self.base_url
        self.simple_client = Salesforce(session_id=access_token, instance_url=instance_url,
                                        domain=domain, version=self.api_version)

    def _init_bulk1_client(self, access_token: str, instance_url: str):
        instance_url = self.base_url
        self.bulk1_client = LegacyBulkClient(session_id=access_token, instance_url=instance_url,
                                             api_version=self.api_version)

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

    @backoff.on_exception(backoff.expo, BulkApiError, max_tries=3, on_backoff=_backoff_handler)
    def create_job_v1(self, object_name=None, operation=None, contentType='CSV',
                      concurrency=None, external_id_name=None, pk_chunking=False, assignement_id=None):
        assert (object_name is not None)
        assert (operation is not None)

        extra_headers = {}
        if pk_chunking:
            if pk_chunking is True:
                pk_chunking = u'true'
            elif isinstance(pk_chunking, int):
                pk_chunking = u'chunkSize=%d;' % pk_chunking
            else:
                pk_chunking = text_type(pk_chunking)

            extra_headers['Sforce-Enable-PKChunking'] = pk_chunking

        if assignement_id:
            extra_headers['assignmentRuleId'] = assignement_id

        doc = self.bulk1_client.create_job_doc(object_name=object_name,
                                               operation=operation,
                                               contentType=contentType,
                                               concurrency=concurrency,
                                               external_id_name=external_id_name)

        resp = requests.post(self.bulk1_client.endpoint + "/job",
                             headers=self.bulk1_client.headers(extra_headers),
                             data=doc)
        self.bulk1_client.check_status(resp)

        tree = ET.fromstring(resp.content)
        job_id = tree.findtext("{%s}id" % self.bulk1_client.jobNS)
        self.bulk1_client.jobs[job_id] = job_id
        self.bulk1_client.job_content_types[job_id] = contentType

        return job_id

    def close_job_v1(self, job_id):
        self.bulk1_client.close_job(job_id)

    @backoff.on_exception(backoff.expo, BulkApiError, max_tries=4, on_backoff=_backoff_handler)
    def get_batch_result_v1(self, job, csv_iter):
        batch = self.retry_post_batch_v1(job, csv_iter)
        try:
            self.retry_wait_for_batch_v1(job, batch)
        except BulkApiError as e:
            logging.warning(f"Batch ID '{batch}' failed: {e}")
        return self.bulk1_client.get_batch_results(batch)

    @backoff.on_exception(backoff.expo, ConnectionError, max_tries=3, on_backoff=_backoff_handler)
    def retry_post_batch_v1(self, job, csv_iter):
        return self.bulk1_client.post_batch(job, csv_iter)

    @backoff.on_exception(backoff.expo, Exception, max_tries=3, on_backoff=_backoff_handler)
    def retry_wait_for_batch_v1(self, job, batch):
        self.bulk1_client.wait_for_batch(job, batch)


class LegacyBulkClient(SalesforceBulk):
    def __init__(self, session_id: str, instance_url: str, api_version: str):

        if instance_url[0:4] == 'http':
            self.endpoint = instance_url
        else:
            self.endpoint = "https://" + instance_url
        self.endpoint += "/services/async/%s" % api_version
        self.sessionId = session_id
        self.jobNS = 'http://www.force.com/2009/06/asyncapi/dataload'
        self.jobs = {}  # dict of job_id => job_id
        self.batches = {}  # dict of batch_id => job_id
        self.job_content_types = {}  # dict of job_id => contentType
        self.batch_statuses = {}
        self.API_version = api_version
