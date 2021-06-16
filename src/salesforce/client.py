from urllib.parse import urlparse

from salesforce_bulk import SalesforceBulk
from salesforce_bulk.salesforce_bulk import DEFAULT_API_VERSION
from six import text_type
import requests
import xml.etree.ElementTree as ET
from retry import retry

NON_SUPPORTED_BULK_FIELD_TYPES = ["address", "location", "base64", "reference"]


class SalesforceClient(SalesforceBulk):
    def __init__(self, sessionId=None, host=None, username=None, password=None,
                 API_version=DEFAULT_API_VERSION, sandbox=False,
                 security_token=None, organizationId=None, client_id=None, domain=None):

        super().__init__(sessionId, host, username, password,
                         API_version, sandbox,
                         security_token, organizationId, client_id, domain)

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
