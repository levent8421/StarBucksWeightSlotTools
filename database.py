from MySQLdb import connect


class DatabaseConnection:
    def __init__(self, user, password, db_name, host='localhost', port=3306):
        self._host = host
        self._port = port
        self._db_name = db_name
        self._user = user
        self._password = password
        self._connection = None
        self._cursor = None

    def connect(self):
        self._connection = connect(host=self._host,
                                   port=self._port,
                                   user=self._user,
                                   password=self._password,
                                   database=self._db_name)
        self._cursor = self._connection.cursor()

    def close(self):
        self._cursor.close()

    def exec_select(self, sql, *args):
        sql = sql % args
        size = self._cursor.execute(sql)
        rows = self._cursor.fetchall()
        return size, rows

    def exec_update(self, sql, *args):
        sql = sql % args
        return self._cursor.execute(sql)
