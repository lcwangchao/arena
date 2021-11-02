import mysql.connector


class Database:
    def __init__(self, *, user=None, host='127.0.0.1', port=4000):
        self._user = user
        self._host = host
        self._port = port

    def conn(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(**self._conn_args())

    def _conn_args(self):
        args = {}
        if self._user:
            args['user'] = self._user
        if self._host:
            args['host'] = self._host
        if self._port:
            args['port'] = self._port
        return args
