# from src.utils.logger import logger
# from pyhive import hive
#
# logger = logger.getChild(__name__)
#
#
# class HiveConnector:
#     def __init__(self, options):
#         self.connection = None
#         self.options = options
#
#     def init(self):
#         logger.debug('mysql connection started: %s', self.options['host'])
#         try:
#             connection = hive.Connection(
#                 host='your_hive_server_hostname',
#                 port=10000,
#                 username='your_username',
#                 password='your_password',
#                 database='your_database'
#             )
#
#             logger.info('hive connection success: %s', self.options['host'])
#         except Exception as error:
#             logger.error('hive connection %s, error: %s', self.options['host'], error)
