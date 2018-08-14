from twisted.internet import reactor
from twisted.web.client import Agent, HTTPConnectionPool, readBody
from twisted.web.http_headers import Headers


class IllustrationDownloader(object):

    def __init__(self, host, port=None, pool_maxsize=2, scheme='http://'):
        self.scheme = scheme
        self.host = host
        self.port = port
        self._pool = HTTPConnectionPool(reactor)
        self._pool.maxPersistentPerHost = pool_maxsize
        self.agent = Agent(reactor, pool=self._pool)

    def fetch(self, uri, headers=None):
        headers = Headers() if headers is None else headers.copy()
        headers.setRawHeaders(b'referer', ['https://www.pixiv.net/'])
        headers.setRawHeaders(b'host', [self.host])

        if (self.port == 80 and self.scheme == 'http://') \
                or (self.port == 443 and self.scheme == 'https://') \
                or self.port is None:
            url = self.scheme + self.host
        else:
            url = self.scheme + self.host + ':' + str(self.port)

        url = url + uri

        dfd = self.agent.request('GET', url, headers)
        dfd.addErrback(self.on_failure)

        def _wait_body(response):
            body_dfd = readBody(response)
            body_dfd.addCallback(lambda b: setattr(response, 'body', b))
            body_dfd.addCallback(lambda _: response)
            return body_dfd

        dfd.addCallback(_wait_body)

        return dfd

    def fetch_by_request(self, request):
        uri = request.uri
        headers = request.requestHeaders
        if not uri.startswith('/'):
            uri = '/' + uri

        return self.fetch(uri, headers)

    def on_failure(self, reason):
        return reason
