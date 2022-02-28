
import mysql.connector
import pandas as pd
from collections import defaultdict
from mysql.connector import Error


class Sql:
    connInitializes = 0

    def __init__(self, **k):
        try:
            self.conn = mysql.connector.connect(
                user=k['user'],
                host=k['host'],
                password=k['password'],
                database=k['database']
            )
        except Error as e:
            print(f"Error in connection : {e}")
        try:
            if self.conn.is_connected():
                print(f"connected : {self.conn.get_server_info()}")
                self.cursor = self.conn.cursor()
        except AttributeError as e:
            print(f"Error Occured: {e} .Check the Connection settings")
        except Error as e:
            print(f"error occured : {e}")

    def test(self, tablename):
        query = f"select * from {tablename}"
        try:
            allRecords = self.cursor.execute(query)
            result = self.cursor.fetchall()
            print(result)
        except Error as e:
            print(f"error occured: {e}")
        except AttributeError as e:
            print(f"error occured: {e}")

    def fetchData(self, tablename, *columns, **where):
        columnsData = None
        whereClauseList = []
        whereClause = None
        if len(columns) > 0:
            columnsData = ",".join(columns)
        if where.keys():
            print("checking where")
            for key in where:
                if isinstance(where[key], int) or isinstance(where[key], float):
                    whereClauseList.append(f"{key}={where[key]}")
                else:
                    whereClauseList.append(f"{key}='{where[key]}'")

            whereClause = " and ".join(whereClauseList)
            print(whereClause)

        if columnsData == None and whereClause == None:
            query = f"select * from {tablename}"
            result = pd.read_sql(query, self.conn)
            # self.cursor.execute(query)
            # result = self.cursor.fetchall()
            print(result)
        elif columnsData != None and whereClause == None:
            print(f"prionting column data {columnsData}")
            query = f"select {columnsData} from {tablename}"
            result = pd.read_sql(query, self.conn)
            # self.cursor.execute(query)
            # result = self.cursor.fetchall()
            print(result)
        elif whereClause != None and columnsData == None:
            query = f"select * from {tablename} where {whereClause}"
            print(query)
            result = pd.read_sql(query, self.conn)
            # self.cursor.execute(query)
            # result = self.cursor.fetchall()
            print(result)
        elif whereClause != None and columnsData != None:
            query = f"select {columnsData} from {tablename} where {whereClause}"
            result = pd.read_sql(query, self.conn)
            print(result)
        else:
            result = []

    def TableExist(self, tablename):
        r = f'select count(*) from information_schema.tables where table_schema=DATABASE() and table_name="{tablename}"'
        self.cursor.execute(r)
        p = self.cursor.fetchall()[0][0]
        return p

    def createTable(self, tablename, **k):
        r = f'select count(*) from information_schema.tables where table_schema=DATABASE() and table_name="{tablename}"'
        self.cursor.execute(r)
        p = self.cursor.fetchall()[0][0]
        if p == 0:
            d = []
            for key in k:
                d.append(f"{key} {' '.join(k[key])}")
            print(d)
            qstr = f"({','.join(d)})"
            print(f"{qstr}")
            try:
                statement = f"create table {tablename} {qstr}"
                self.cursor.execute(statement)
            except Error as e:
                print(f"Error occured : {e}")
            else:
                print(f"SQL Table with the table name {tablename} created")
        else:
            print(f"SQL Table with name {tablename} already exist")

    def showColumns(self):
        self.cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='fastapi' AND TABLE_NAME='testing'")
        result = self.cursor.fetchall()
        print(result)

    def fetchData(self, *args, **kwargs):
        tableName = None
        columns = None
        whereQueryList = []
        whereQuery = None
        queryStatement = None
        orderby = ''
        whereQuery2 = ''
        if args[0]:
            tableName = args[0]
        if args[1:]:
            columns = ",".join(args[1:])
        tableExist = self.TableExist(tableName)
        if tableExist == 1:
            if kwargs.keys():
                for key, value in kwargs.items():
                    if (isinstance(value, int) or isinstance(value, float)) and key != 'limit' and key != 'orderBy':
                        whereQueryList.append(f"{key}={value}")
                    if(isinstance(value, str)) and key != 'limit' and key != 'orderBy':
                        whereQueryList.append(f"{key}='{value}'")
                    if key == 'limit' and len(value) >= 2:
                        whereQuery2 += f" {key} {value[0]},{value[1]}"
                    elif key == 'limit' and len(value) == 1:
                        whereQuery2 += f" {key} {value[0]}"
                    if key == 'orderBy' and len(value) > 0:
                        for i in value:
                            orderby += f"{i} "
                        whereQuery2 += f" order by {orderby}"

                if len(whereQueryList) > 0:
                    whereQuery = " and ".join(whereQueryList)

            print(f"whereQuery2")

            if columns == None and len(whereQueryList) == 0:
                queryStatement = f"SELECT * FROM {tableName} {whereQuery2}"

            elif len(whereQueryList) > 0 and columns == None:
                queryStatement = f"SELECT * FROM {tableName} WHERE {whereQuery} {whereQuery2}"

            elif columns != None and len(whereQueryList) == 0:
                queryStatement = f"SELECT {columns} FROM {tableName} {whereQuery2}"

            elif columns != None and len(whereQueryList) > 0:

                queryStatement = f"SELECT {columns} FROM {tableName} WHERE {whereQuery} {whereQuery2}"
            else:
                print("Please enter the valid query")
            # if columns != None and len(whereQueryList) == 0 and whereQuery2:
            #     queryStatement = f"select {columns} from {tableName} {whereQuery2}"

            print(queryStatement)
            df = pd.read_sql(queryStatement, self.conn)
            print(df)
        else:
            print(f"Table with table name {tableName} does not exist")


sql = Sql(user='root', host='localhost', password='owl', database='fastapi')
# sql.fetchData("blogg")
# sql.createTable("demo1", name=("varchar(20)", "not null"), roll=("int",))
sql.fetchData("testing", orderBy=('ResponseId DESC',), limit=(20, 30))
