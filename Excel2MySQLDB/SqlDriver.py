import mysql.connector


class SqlDriver:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="20021121",
            database="corpus_database"
        )

    def execute_read(self, sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()

        return results

    def execute_write(self, sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)

        self.conn.commit()


