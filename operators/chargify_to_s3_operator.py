from chargify_plugin.hooks.chargify_hook import ChargifyHook

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks import S3Hook

from flatten_json import flatten
import logging
import json


class ChargifyToS3Operator(BaseOperator):
    """

    NOTE: This operator returns newline delimited JSON.

    S3 To Chargify Operator
        :param chargify_conn_id:    The destination redshift connection id.
        :type chargify_conn_id:     string
        :param endpoint:            The endpoint from which data is being
                                    requested.
        :type endpoint:             string
        :param s3_conn_id:          The s3 connection id.
        :type s3_conn_id:           string
        :param s3_bucket:           The destination s3 bucket.
        :type s3_bucket:            string
        :param s3_key:              The destination s3 key.
        :type s3_key:               string
        :param payload:             Additional parameters you're making in
                                    the request.
        :type payload:              string

    """

    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(self,
                 chargify_conn_id,
                 endpoint,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 payload={},
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.chargify_conn_id = chargify_conn_id
        self.endpoint = endpoint
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.payload = payload

    def execute(self, context):

        def make_request(conn_id, endpoint, payload=None):
            return (ChargifyHook(conn_id)
                    .run(endpoint, payload)
                    .json())

        output = []
        final_payload = {'per_page': 200, 'page': 1}
        for param in self.payload:
            final_payload[param] = self.payload[param]

        response = make_request(self.chargify_conn_id,
                                self.endpoint,
                                final_payload)
        while response:
            output.extend(response)
            final_payload['page'] += 1
            response = make_request(self.chargify_conn_id,
                                    self.endpoint,
                                    final_payload)

            logging.info('Retrieved: ' + str(final_payload['per_page']
                                             * final_payload['page']))

        output = [record[self.endpoint[:-1]] for record in output]
        output = '\n'.join([json.dumps(flatten(record)) for record in output])

        s3 = S3Hook(s3_conn_id=self.s3_conn_id)
        s3.load_string(
            string_data=output,
            bucket_name=self.s3_bucket,
            key=self.s3_key,
            replace=True
        )
        s3.connection.close()
