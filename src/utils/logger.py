import logging

log = logging.getLogger('werkzeug')
log.setLevel(logging.DEBUG)
log.disabled = True


logging.basicConfig(format='%(asctime)s :: [%(process)d] :: %(name)s :: %(levelname)s :: %(message)s', level="DEBUG")
logger = logging.getLogger()


logger.debug('Initialized logging')
