import pandas as pd
import xlrd
from db_readwrite import DbReadWrite
from MockDataCreator import MockDataCreator
from CraigslistPriceScrapingTool import CraigslistScraper
from CommodityDataQueriesForStats import commodity_data_queries

def testReadStateInfoTable():
    # Test reader, get STATE_INFO
    dbReadWrite = DbReadWrite()
    conn = dbReadWrite.getConnection()
    querySql = "SELECT * FROM STATE_INFO;"
    dataFrame = dbReadWrite.executeQueryToFrame(querySql, conn)
    for index, row in dataFrame.iterrows():
        print(row['STATE_ID'], row['STATE_NAME'], row['POP'], row['POP_RANK'], row['POP_PERCENT'], row['GDP_DOLLARS'],
              row['GDP_RANK'], row['GDP_PERCENT'], row['GDP_PERCENT'], row['GDP_PER_CAPITA'], row['REGION_SK_FED'],
              row['REGION_SK_DIST'])

def testWriteCommodityPriceInfo():
    # Test writer for CommodityPriceInfo
    dbReadWrite = DbReadWrite()
    conn = dbReadWrite.getConnection()
    mockDataCreator = MockDataCreator()
    mockDataFrame = mockDataCreator.getMockCommodityPriceInfoDataFrame()
    dbReadWrite.writeCommodityPriceInfo(mockDataFrame)

def testDataReadWrite():
    # Main test area.
    # Todo: add exceptions on error
    testReadStateInfoTable()
    testWriteCommodityPriceInfo()
    testReadCommodityPriceInfo()

# This is a test function, probably won't use in production.
def testReadCommodityPriceInfo():
    # Now read and print, see if we got the data into the database.
    dbReadWrite = DbReadWrite()
    conn = dbReadWrite.getConnection()
    querySql = "SELECT * FROM COMMODITY_PRICE_INFO;"
    dataFrameFromDB = dbReadWrite.executeQueryToFrame(querySql, conn)
    print("Data read from DB: ", dataFrameFromDB)

def testGDPRankQueries():
    # Finally, rank by GDP dollars.
    # Use similar strategy to pop rank.
    # highest gdp rank states
    db_read_write = DbReadWrite()
    conn = db_read_write.getConnection()
    data_queries = commodity_data_queries()

    print('Reading gdp rank query for rank 1: ')
    gdp_rank_1_price_data = db_read_write.executeQueryToFrame(data_queries.get_gdp_rank_query(1), conn)
    print(gdp_rank_1_price_data)

    # next highest population rank states
    print('Reading gdp rank query for rank 2: ')
    gdp_rank_2_price_data = db_read_write.executeQueryToFrame(data_queries.get_gdp_rank_query(2), conn)
    print(gdp_rank_2_price_data)

    # lowest pop rank states.
    print('Reading gdp rank query for rank 9: ')
    gdp_rank_9_price_data = db_read_write.executeQueryToFrame(data_queries.get_gdp_rank_query(9), conn)
    print(gdp_rank_9_price_data)

    # lowest pop rank states.
    print('Reading gdp rank query for rank 10: ')
    gdp_rank_10_price_data = db_read_write.executeQueryToFrame(data_queries.get_gdp_rank_query(10), conn)
    print(gdp_rank_10_price_data)

def testPOPRankQueries():
    # Finally, rank by GDP dollars.
    # Use similar strategy to pop rank.
    # highest gdp rank states
    db_read_write = DbReadWrite()
    conn = db_read_write.getConnection()
    data_queries = commodity_data_queries()

    # Population rank queries.
    # highest population rank states
    print('Reading pop rank query for rank 1: ')
    pop_rank_1_price_data = db_read_write.executeQueryToFrame(data_queries.get_pop_rank_query(1), conn)
    print(pop_rank_1_price_data)

    # next highest population rank states
    print('Reading pop rank query for rank 2: ')
    pop_rank_2_price_data = db_read_write.executeQueryToFrame(data_queries.get_pop_rank_query(2), conn)
    print(pop_rank_2_price_data)

    # lowest pop rank states.
    print('Reading pop rank query for rank 9: ')
    pop_rank_9_price_data = db_read_write.executeQueryToFrame(data_queries.get_pop_rank_query(9), conn)
    print(pop_rank_9_price_data)

    # lowest pop rank states.
    print('Reading pop rank query for rank 10: ')
    pop_rank_10_price_data = db_read_write.executeQueryToFrame(data_queries.get_pop_rank_query(10), conn)
    print(pop_rank_10_price_data)

def testStateFetchQueries():
    # Run a few test loads for state
    db_read_write = DbReadWrite()
    conn = db_read_write.getConnection()
    data_queries = commodity_data_queries()

    print('Reading CA price data: ')
    ca_price_data = db_read_write.executeQueryToFrame(data_queries.get_state_query('CA'), conn)
    print(ca_price_data)

    print('Reading MA price data: ')
    ma_price_data = db_read_write.executeQueryToFrame(data_queries.get_state_query('MA'), conn)
    print(ma_price_data)

    print('Reading RI price data: ')
    ri_price_data = db_read_write.executeQueryToFrame(data_queries.get_state_query('RI'), conn)
    print(ri_price_data)

def testRegionFetchQueries():
    # Run a few test loads for state
    db_read_write = DbReadWrite()
    conn = db_read_write.getConnection()
    data_queries = commodity_data_queries()

    print('Reading Region 1 price data: ')
    region_1_price_data = db_read_write.executeQueryToFrame(data_queries.get_region_1_query(), conn)
    print(region_1_price_data)

    print('Reading Region 2 price data: ')
    region_2_price_data = db_read_write.executeQueryToFrame(data_queries.get_region_2_query(), conn)
    print(region_2_price_data)

    print('Reading Region 3 price data: ')
    region_3_price_data = db_read_write.executeQueryToFrame(data_queries.get_region_3_query(), conn)
    print(region_3_price_data)

    print('Reading Region 4 price data: ')
    region_4_price_data = db_read_write.executeQueryToFrame(data_queries.get_region_4_query(), conn)
    print(region_4_price_data)