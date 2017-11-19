from airflow.hooks.http_hook import HttpHook


class ChargifyHook(HttpHook):
    """
    Hook for Chargify.

    Inherits from the base HttpHook to make a request to Chargify.
    Uses basic authentication via an API Key that should
    be stored in the 'Login' field in the Airflow Connection panel
    with an 'X' as the password.

    Defaults to GET requests.

    """

    def __init__(self, chargify_conn_id, method='GET'):
        super().__init__(method, http_conn_id=chargify_conn_id)

    def get_conn(self, headers=None):
        session = super().get_conn(headers=headers)
        return session

    def run(self, endpoint, payload=None):
        self.endpoint = '{0}.json'.format(endpoint)
        # Hard code hook to return JSON
        headers = {"Accept": "application/json"}
        return super().run(self.endpoint, data=payload, headers=headers)
