from source_config import Config
from boto.s3.connection import S3Connection
import credentials, logging, pgdb, cx_Oracle
logger = logging.getLogger(__name__)

class DataSource:

    def __init__(self, dsn):
        self.__dsn = Config().get(dsn)
        self.__credentials = credentials.get_credentials(dsn)
        self.__cnx = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # all connection types supported by class so far implement disconnection method close() identically
        self.disconnect()

    def connect(self):

        if self.__cnx:
            return self.__cnx

        conn_type = {
            'redshift': self.__create_red_connection,
            'oracle': self.__create_ora_connection,
            's3': self.__create_s3_connection
        }

        func = conn_type[self.__dsn.type]

        logger.info("Connecting to database: [{db_dsn}]...".format(db_dsn=self.__dsn.data_source))
        self.__cnx = func()
        logger.info("Connected.")

        if self.__dsn.type == 's3':
            # For S3 source return original connection object
            return self.__cnx
        else:
            # For non-S3 source return self object in order to use own cursor()
            return self

    def disconnect(self):
        if self.__cnx:
            self.__cnx.close()
            self.__cnx = None
            logger.info("Disconnected.")

    def cursor(self):

        conn_type = {
            'redshift': DataSourcePGDBCursor,
            'oracle': DataSourceOraCursor
        }

        if self.__cnx:
            return conn_type[self.__dsn.type](self.__cnx)
        else:
            return self.connect().cursor()

    def __create_red_connection(self):
        user, password = self.__credentials
        conn = pgdb.connect(database=self.__dsn.dbname, host=self.__dsn.host + ':' + str(self.__dsn.port), user=user, password=password)
        return conn


    def __create_ora_connection(self):
        user, password = self.__credentials
        dsn_tns = cx_Oracle.makedsn(self.__dsn.host, self.__dsn.port, self.__dsn.dbname)
        return cx_Oracle.connect(user, password, dsn_tns)

    def __create_s3_connection(self):
        access_key, secret_key = self.__credentials
        return S3Connection(access_key,secret_key)

class DataSourcePGDBCursor(pgdb.Cursor):

    def fetchall_dict(self):
        return [{col[0]: row[i] for i, col in enumerate(self.description)} for row in super(DataSourcePGDBCursor, self).fetchall()]

    def __execute(self, operation, params = None):

        logger.info("Executing statement...")
        logger.debug(operation)
        super(DataSourcePGDBCursor, self).execute(operation, params)
        logger.info("Statement complete.")

        return self

    def __execute_from_file(self, file_name, params = None):

        with open(file_name,'r') as operation:
            return self.__execute(operation.read(), params)

    def execute(self, operation, params = None):
        import os.path
        if os.path.isfile(operation):
            return self.__execute_from_file(operation, params)
        else:
            return self.__execute(operation, params)

class DataSourceOraCursor(cx_Oracle.Cursor):

    def fetchall_dict(self, size=None):
        if size is None:
            size = self.arraysize
        return [{col[0]: row[i] for i, col in enumerate(self.description)} for row in super(DataSourceOraCursor, self).fetchmany(size)]

    def __execute(self, operation, params = None):

        logger.info("Executing statement...")
        operation = operation.format(params)
        logger.debug(operation)
        super(DataSourceOraCursor, self).execute(operation)
        logger.info("Statement complete.")

        return self

    def __execute_from_file(self, file_name, params = None):

        with open(file_name,'r') as operation:
            return self.__execute(operation.read(), params)

    def execute(self, operation, params = None):
        import os.path
        if os.path.isfile(operation):
            return self.__execute_from_file(operation, params)
        else:
            return self.__execute(operation, params)
