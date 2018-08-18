import datetime
import logging

from twisted.internet import reactor, defer
from twisted.web.proxy import ReverseProxyResource
from twisted.web.resource import NoResource
from twisted.web.server import NOT_DONE_YET

from pixiv_fetcher.downloader import IllustrationDownloader
from pixiv_fetcher.utils.pixiv import parse_pximg_url
from pixiv_fetcher.utils.time import datetime2gmt

logger = logging.getLogger(__name__)


class PixivImageProxyResource(ReverseProxyResource):

    def __init__(self, host, path, port=80, cache=None, pool_maxsize=4,
                 downloader=None, reactor=reactor):
        path = path[:-1] if path.endswith('/') else path
        paths = path.split('/')

        root_path = paths[0]
        ReverseProxyResource.__init__(self, host, port, root_path, reactor)
        self._sub_paths = paths[1:]

        self._downloader = downloader \
            or IllustrationDownloader(host=host, pool_maxsize=pool_maxsize)

        self._cache = cache

    def getChild(self, path, request):
        if not hasattr(request, 'path_depth'):
            setattr(request, 'path_depth', -1)

        request.path_depth += 1

        depth = request.path_depth
        if depth < len(self._sub_paths) and self._sub_paths[depth] != path:
            return NoResource('Not Found.')

        return self

    def render(self, request):
        client = request.client
        uri = request.uri

        logger.info('%s %s %s', request.method, uri, client)

        if request.requestHeaders.hasHeader('If-Modified-Since'):
            self._return_304(request)
            logger.info('HTTP304 %s', client)
            return NOT_DONE_YET
        elif self._cache:
            data = self._cache.get(uri)

            if data:
                self._return_data(request, data)
                return NOT_DONE_YET

        dfd = self._downloader.fetch_by_request(request)

        if self._cache:
            self._cache_response(request, dfd)

        dfd.addCallbacks(callback=self._return_response, callbackArgs=(request,),
                         errback=self._handle_failure, errbackArgs=(request,))

        return NOT_DONE_YET

    def _cache_response(self, request, deferred):
        dfd = defer.Deferred()
        dfd.addCallback(lambda _: deferred)

        def _process(response):
            if response.code == 200:
                body = getattr(response, 'body', None)
                if body:
                    key = request.uri
                    self._cache.set(key, body)

        def _fail(reason, request):
            logger.exception(reason)
            return reason

        dfd.addCallback(_process)
        dfd.addErrback(_fail, request)
        reactor.callInThread(dfd.callback, None)

    def _send_cache_headers(self, request, last_modified=None):
        request.responseHeaders.setRawHeaders('Cache-Control', ['max-age=31536000'])

        dt = datetime.datetime.now()
        expires = dt + datetime.timedelta(seconds=31536000)
        request.responseHeaders.setRawHeaders('Date', [datetime2gmt(dt)])
        request.responseHeaders.setRawHeaders('Expires', [datetime2gmt(expires)])

        lm = request.requestHeaders.getRawHeaders('If-Modified-Since', [])
        lm = lm or ([datetime2gmt(last_modified)] if last_modified else None)
        if lm:
            request.responseHeaders.setRawHeaders('Last-Modified', lm)

    def _return_data(self, request, data):
        img_info = parse_pximg_url(request.uri)
        request.setResponseCode(200, 'OK')
        # request.responseHeaders.setRawHeaders('Access-Control-Allow-Origin', ['*'])
        request.responseHeaders.setRawHeaders('Content-Type', ['image/'+img_info.extension])
        self._send_cache_headers(request, last_modified=img_info.datetime)
        request.write(data)
        request.finish()

    def _return_304(self, request):
        request.setResponseCode(304, 'Not Modified')
        request.responseHeaders.setRawHeaders('X-Content-Type-Options', ['nosniff'])

        self._send_cache_headers(request)

        request.responseHeaders.setRawHeaders('Server', ['nginx'])
        request.finish()

    def _return_response(self, response, request):
        client = request.client
        logger.info('HTTP%d %s %s', response.code, response.phrase, client)
        request.setResponseCode(response.code, response.phrase)

        for key, values in response.headers.getAllRawHeaders():
            request.responseHeaders.setRawHeaders(key, values)

        request.write(response.body)
        request.finish()

        return response

    def _handle_failure(self, reason, request):
        logger.exception(reason)
        request.setResponseCode(500, b"Internal Server Error")
        request.responseHeaders.addRawHeader(b"Content-Type", b"text/html")
        request.write(b"<H1>Internal Server Error</H1>")
        request.finish()
