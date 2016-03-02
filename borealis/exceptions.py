import json


class SolrHTTPException(Exception):
    def __init__(self, http_exception):
        self.http_exception = http_exception
        self.response_code = http_exception.code

        self.response, self.decoded, self.msg = self._parse_exc(http_exception)

    def _parse_exc(self, http_exception):
        """Attempt to read and JSON decode response from server"""
        response = ""
        try:
            response = http_exception.read()
        except:
            pass

        decoded = {}
        try:
            decoded = json.loads(response)
        except ValueError:
            pass

        msg = decoded.get('exception', {}).get('msg', None)
        if not msg:
            msg = decoded.get('error', {}).get('msg', None)

        return response, decoded, msg

    def __str__(self):
        if self.msg:
            return "SolrException(%s, %s)" % (self.response_code, self.msg)
        else:
            return "SolrException(%s, %s)" % (self.response_code, self.response)
