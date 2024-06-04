import psycopg2
from psycopg2 import sql, Error
import pandas as pd
import logging

class PostgresOperation:
    __connection = None
    __cursor = None

    def __init__(self, dbname: str, user: str, password: str, host: str):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.logger = logging.getLogger(__name__)

    def create_connection(self):
        try:
            self.__connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host
            )
            self.__connection.autocommit = True
            self.__cursor = self.__connection.cursor()
            self.logger.info("PostgreSQL connection is opened.")
        except Error as e:
            self.logger.error(f"Error: {e}")
            return None
        return self.__connection

    def table_exists(self, table_name: str) -> bool:
        try:
            self.__cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", (table_name,))
            return self.__cursor.fetchone()[0]
        except Error as e:
            self.logger.error(f"Error: {e}")
            return False

    def create_table(self, table_name: str, schema: str):
        if not self.table_exists(table_name):
            try:
                self.__cursor.execute(sql.SQL("CREATE TABLE {} ({})").format(
                    sql.Identifier(table_name),
                    sql.SQL(schema)
                ))
                self.logger.info(f"Table '{table_name}' has been created.")
            except Error as e:
                self.logger.error(f"Error: {e}")

    def insert_record(self, table_name: str, record: dict):
        columns = list(record.keys())
        values = list(record.values())
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(values))
        )
        try:
            self.__cursor.execute(query, tuple(values))
            self.logger.info("Record inserted successfully.")
        except Error as e:
            self.logger.error(f"Error: {e}")

    def bulk_insert(self, datafile: str, table_name: str):
        if datafile.endswith('.csv'):
            dataframe = pd.read_csv(datafile, encoding='utf-8')
        elif datafile.endswith(".xlsx"):
            dataframe = pd.read_excel(datafile, encoding='utf-8')
        else:
            raise ValueError("Unsupported file format")

        for _, row in dataframe.iterrows():
            self.insert_record(table_name, row.to_dict())

    def read_data(self, table_name: str, filter: str = None, projection: str = None,
                  sort: str = None, limit: int = None, skip: int = None):
        query = sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(projection) if projection else sql.SQL('*'),
            sql.Identifier(table_name)
        )
        if filter:
            query += sql.SQL(" WHERE {}").format(sql.SQL(filter))
        if sort:
            query += sql.SQL(" ORDER BY {}").format(sql.SQL(sort))
        if limit:
            query += sql.SQL(" LIMIT {}").format(sql.Literal(limit))
        if skip:
            query += sql.SQL(" OFFSET {}").format(sql.Literal(skip))

        try:
            self.__cursor.execute(query)
            records = self.__cursor.fetchall()
            return records
        except Error as e:
            self.logger.error(f"Error: {e}")
            return []

    def delete_record(self, table_name: str, condition: str):
        try:
            query = sql.SQL("DELETE FROM {} WHERE {}").format(
                sql.Identifier(table_name),
                sql.SQL(condition)
            )
            self.__cursor.execute(query)
            self.logger.info("Record(s) deleted successfully.")
        except Error as e:
            self.logger.error(f"Error: {e}")

    def close_connection(self):
        if self.__cursor is not None:
            self.__cursor.close()
        if self.__connection is not None:
            self.__connection.close()
            self.logger.info("PostgreSQL connection is closed.")

def main():
    # Replace these with your actual database credentials
    dbname = 'test'
    user = 'your_username'
    password = 'postgreysql'
    host = '7301'

    # Initialize the PostgresOperation class
    db = PostgresOperation(dbname, user, password, host)

    # Create a connection
    connection = db.create_connection()
    if connection is None:
        db.logger.error("Failed to create database connection.")
        return

    # Define your table schema here (example: 'id SERIAL PRIMARY KEY, name VARCHAR(100), age INT')
    table_schema = 'your_table_schema'
    table_name = 'your_table_name'

    # Create a table
    db.create_table(table_name, table_schema)

    # Insert a record (example record)
    record = {'name': 'John Doe', 'age': 30}
    db.insert_record(table_name, record)

    # Bulk insert from a file
    datafile = 'path_to_your_file.csv'
    db.bulk_insert(datafile, table_name)

    # Read data
    records = db.read_data(table_name)
    for record in records:
        print(record)

    # Delete a record
    db.delete_record(table_name, "name = 'John Doe'")

    # Close the connection
    db.close_connection()

if __name__ == '__main__':
    main()
