
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

    def createTableFromCSV(self, csvFile, tablename, **kw):
        tableExist = self.TableExist(tablename)
        if tableExist == 0:
            k = defaultdict()
            c = []
            columns = pd.read_csv(csvFile).columns.tolist()
            for i in columns:
                k[i] = i
            if 'primarykey' in kw.keys() and kw['primarykey'] in columns:
                k['primarykey'] = f"primary key({kw['primarykey']})"

            if 'foreignkey' in kw.keys() and kw['foreignkey'][0] in columns:
                k['foreignkey'] = f"foreign key({kw['foreignkey'][0]}) references {kw['foreignkey'][1]}"

            for key in k:
                if 'integer' in kw.keys() and key in kw['integer']:
                    k[key] = f"{k[key]} int"
                if 'float' in kw.keys() and key in kw['float']:
                    k[key] = f"{k[key]} decimal(10,2)"
                if 'notnull' in kw.keys() and key in kw['notnull']:
                    k[key] = f"{k[key]} not null"
                if 'autoincrement' in kw.keys():
                    k[key] = f"{k[key]} auto_increment"

                if ('integer' in kw.keys() and key not in kw['integer'] and 'float' in kw.keys() and key not in kw['float']) and (key not in ['primarykey', 'foreignkey']):
                    k[key] = f"{k[key]} varchar(300)"
                elif ('integer' in kw.keys() and key not in kw['integer'] and 'float' not in kw.keys()) and (key not in ['primarykey', 'foreignkey']):
                    k[key] = f"{k[key]} varchar(300)"
                elif ('integer' not in kw.keys() and 'float' in kw.keys() and key not in kw['float']) and (key not in ['primarykey', 'foreignkey']):
                    k[key] = f"{k[key]} varchar(300)"

                c.append(k[key])
            sqltabledata = f'({",".join(c)})'
            print(sqltabledata)
            sqlquery = f'create table if not exists {tablename} {sqltabledata}'
            self.cursor.execute(sqlquery)
            print()
            print(f"generated sql query : {sqlquery}")
            print()
            print(f"CREATED TABLE WITH TABLENAME {tablename}")
        else:
            print("Table with tablename {tablename} already exist")

    def dbLoad(self, filename, tablename):
        rowCount = 0

        f = pd.read_csv(filename)
        f.fillna('0', inplace=True)
        file = f
        columns = file.columns.tolist()
        c = tuple(columns)
        for i in file.itertuples(index=False, name=None):
            query = f"INSERT INTO {tablename} VALUES {i}"
            # print(query)
            try:
                self.cursor.execute(query)
                self.conn.commit()

                rowCount += 1
                print(f"{rowCount} record added successfully")
            except Error as e:
                print(e)
        print(rowCount)


sql = Sql(user='root', host='localhost', password='owl', database='fastapi')
# sql.fetchData("blogg")
# sql.createTable("demo1", name=("varchar(20)", "not null"), roll=("int",))
# sql.fetchData("testing", 'Country', orderBy=(
#     'ResponseId DESC',), limit=(20, 30))


# try:
#     sql.createTableFromCSV("/home/owl/assetdata.csv", "skillslashdemo", integer=[
#         'assetnum'], float=['purchasedprice'], primarykey='assetnum')
# except Error as e:
#     print(e)

# sql.dbLoad("/home/owl/assetdata.csv", "skillslashdemo")

# sql.fetchData("skillslashdemo", "assetnum",
#               orderby=('assetnum ',), limit=(3,))

try:
    sql.createTableFromCSV("stack.csv", "skill2", integer=[
        'ResponseId'], primarykey='ResponseId', float=['ConvertedCompYearly', 'CompTotal'])
except Error as e:
    print(e)

sql.dbLoad("stack.csv", "skill2")
