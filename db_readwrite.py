import pyodbc
import pandas as pd


# First version just writes to SALE_PRICE_STATS
# Next version, move that out, and pass in via sql
# This one reads data into a data frame.
class DbReadWrite:

    def __init__(self):
        self.server = "LAPTOP-22T8FUG0"
        self.database = "SALE_PRICE_STATS"
        self.userName = "TestUser"
        self.passWord = "TestPassword"

    def setServer(self, serverName):
        self.server = serverName

    def setDatabase(self, databaseName):
        self.database = databaseName

    def setUserName(self, userName):
        self.userName = userName

    def setPassword(self, password):
        self.passWord = password

    # Open a database connection to class configured db
    def getConnection(self):
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=' + self.server + ';'
                                                        'Database=' + self.database + ';'
                                                                                      'Trusted_Connection=yes;')
        return conn

    # --end getConnection -----------------------------

    def getCursor(self, conn):
        cursor = conn.cursor()
        return cursor

    # --end getCursor -----------------------------

    # 'SELECT * FROM STATE_INFO;'
    def executeQuery(self, querySql, cursor):
        cursor.execute(querySql + ';')
        row = cursor.fetchone()
        while row:
            print("This is a row: ", row)
            row = cursor.fetchone()

    # --end executeQuery -----------------------------

    # fetch table into pandas frame.
    def executeQueryToFrame(self, querySql, conn):
        sql_query_frame = pd.read_sql_query(querySql, conn)
        return sql_query_frame

    # --end executeQueryToFrame -----------------------------

    # takes data frame, data read from web.
    # Writes to sql server table COMMODITY_PRICE_INFO
    # Adds SCAN_DATE which is curent date.
    def writeCommodityPriceInfo(self, dataFrame, scanDate):

        conn = self.getConnection()

        # Write line by line to table.
        for index, row in dataFrame.iterrows():
            print('Writing rec: ', row['STATE_ID'], row['AD_SOURCE_INFO_ID_NAME'], row['COMMODITY'],
                  row['ASKING_PRICE'], row['TITLE'], row['NEIGHBORHOOD'],
                  row['SCAN_DATE'])
            row['SCAN_DATE'] = scanDate
            # Current column set is:  STATE_ID, AD_SOURCE_INFO, COMMODITY, ASKING_PRICE, TITLE, NEIGHBORHOOD, SCAN_DATE
            insert_records = '''INSERT INTO COMMODITY_PRICE_INFO(STATE_ID, AD_SOURCE_INFO_ID_NAME, COMMODITY, ASKING_PRICE, 
                        TITLE, NEIGHBORHOOD, SCAN_DATE) VALUES(?, ?, ?, ?, ?, ?, ?) '''
            cursor = self.getCursor(conn)
            cursor.execute(insert_records, row['STATE_ID'], row['AD_SOURCE_INFO_ID_NAME'], row['COMMODITY'],
                           row['ASKING_PRICE'],
                           row['TITLE'], row['NEIGHBORHOOD'], row['SCAN_DATE'])
            # end of for loop.

        conn.commit()
        conn.close()
        # end for.
    # --end writeCommodityPriceInfo -----------------------------
