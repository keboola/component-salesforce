from typing import OrderedDict
from urllib.parse import urlparse

from salesforce_bulk import SalesforceBulk
from salesforce_bulk.salesforce_bulk import DEFAULT_API_VERSION
from simple_salesforce import Salesforce, SFType
from six import text_type
import requests
import xml.etree.ElementTree as ET
from retry import retry

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


class SalesforceClient(SalesforceBulk):
    def __init__(self, sessionId=None, host=None, username=None, password=None,
                 API_version=DEFAULT_API_VERSION, sandbox=False,
                 security_token=None, organizationId=None, client_id=None, domain=None):

        super().__init__(sessionId, host, username, password,
                         API_version, sandbox,
                         security_token, organizationId, client_id, domain)

        if domain is None and sandbox:
            domain = 'test'

        self.simple_client = Salesforce(username=username, password=password, security_token=security_token,
                                        organizationId=organizationId, client_id=client_id,
                                        domain=domain, version=API_version)

        self.host = urlparse(self.endpoint).hostname

    @retry(tries=3, delay=5)
    def create_job(self, object_name=None, operation=None, contentType='CSV',
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

        doc = self.create_job_doc(object_name=object_name,
                                  operation=operation,
                                  contentType=contentType,
                                  concurrency=concurrency,
                                  external_id_name=external_id_name)

        resp = requests.post(self.endpoint + "/job",
                             headers=self.headers(extra_headers),
                             data=doc)
        self.check_status(resp)

        tree = ET.fromstring(resp.content)
        job_id = tree.findtext("{%s}id" % self.jobNS)
        self.jobs[job_id] = job_id
        self.job_content_types[job_id] = contentType

        return job_id

    def get_bulk_fetchable_objects(self):
        all_s_objects = self.simple_client.describe()["sobjects"]
        to_fetch = []
        # Only objects with the 'queryable' set to True and ones that are not in the OBJECTS_NOT_SUPPORTED_BY_BULK are
        # queryable by the Bulk API. This list might not be exact, and some edge-cases might have to be addressed.
        for sf_object in all_s_objects:
            if sf_object.get('queryable') and not sf_object.get('name') in OBJECTS_NOT_SUPPORTED_BY_BULK:
                to_fetch.append({"label": sf_object.get('label'), 'value': sf_object.get('name')})
        return to_fetch

    @retry(tries=3, delay=5)
    def describe_object_w_metadata(self, sf_object: str, api_version):
        salesforce_type = SFType(sf_object, self.sessionId, self.host, sf_version=api_version)

        try:
            object_desc = salesforce_type.describe()
        except ConnectionError:
            # TODO: handle error properly
            pass

        return [(field['name'], field['type']) for field in object_desc['fields']
                if self.is_bulk_supported_field(field)]

    @staticmethod
    def is_bulk_supported_field(field: OrderedDict) -> bool:
        return field["type"] not in NON_SUPPORTED_BULK_FIELD_TYPES
