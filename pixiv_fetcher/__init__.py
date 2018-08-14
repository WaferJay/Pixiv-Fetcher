import logging

from twisted.internet.ssl import ClientContextFactory

from .resource import PixivImageProxyResource


class WebClientContextFactory(ClientContextFactory):

    def getContext(self, hostname, port):
        return ClientContextFactory.getContext(self)


_logger = logging.Logger('pixiv_fetcher')
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter('%(asctime)s - [%(threadName)s'
                                        '(%(thread)d)/%(name)s/%(levelname)s]'
                                        ' - %(message)s'))
_logger.addHandler(_handler)
logging.root.addHandler(_handler)
del logging
