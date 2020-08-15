import pandas as pd
import xlrd
from db_readwrite import DbReadWrite
from MockDataCreator import MockDataCreator
from CraigslistPriceScrapingTool import CraigslistScraper
from CommodityDataQueriesForStats import commodity_data_queries
from pricing_stats import pricing_stats_calculator

################################################################################################################
# Run tester for data read/write


# configuration switches.
# Todo: allow reading from the command line
run_fetch_price_data = False
run_test_queries = False
run_test_read_write = False
run_calc_stats = True


def main():
    print("Pricing data stats calculator V0.1 by Brian Pukmel")

    if (run_fetch_price_data is True):
        # Run scraper.
        craiglist_scraper = CraigslistScraper()
        save_to_database = True
        # If you uncomment one of these, you'll run another fetch
        # It takes about 1 hour to run.
        #
        # tested working This is the development method.
        # This method writes to db if flag is True.
        # CraigsListScraper.run_all_states(save_to_database)

        # Uncomment this to run the scraper for cars data.
        # This version is a little more abstracted.
        # This method writes to db if flag is True.
        craiglist_scraper.run_all_states_by_batch(save_to_database)

    if (run_test_queries is True):
        # Test the query functions.
        # What we'll do in main is use the guts of those
        # functions directly, because main needs data frames.
        testGDPRankQueries()
        testPOPRankQueries()
        testStateFetchQueries()
        testRegionFetchQueries()

    if (run_calc_stats is True):
        # Read region 1 data into data frame
        db_read_write = DbReadWrite()
        conn = db_read_write.getConnection()
        data_queries = commodity_data_queries()

        print('Reading Region 1 price data: ')
        region_1_price_data = db_read_write.executeQueryToFrame(data_queries.get_region_1_query(), conn)
        print(region_1_price_data)

        stats_calculator = pricing_stats_calculator()
        stats_calculator.calc_stats(region_1_price_data)

        print('Reading Region 2 price data: ')
        region_2_price_data = db_read_write.executeQueryToFrame(data_queries.get_region_2_query(), conn)
        stats_calculator.calc_stats(region_2_price_data)

        print('Reading Region 3 price data: ')
        region_3_price_data = db_read_write.executeQueryToFrame(data_queries.get_region_3_query(), conn)
        stats_calculator.calc_stats(region_3_price_data)

        print('Reading Region 4 price data: ')
        region_4_price_data = db_read_write.executeQueryToFrame(data_queries.get_region_4_query(), conn)
        stats_calculator.calc_stats(region_4_price_data)

        # loop over every state, and run stats
        craiglist_scraper = CraigslistScraper()
        state_abbrev_list = craiglist_scraper.get_state_abbrev()
        for state in state_abbrev_list:
            state_price_data = db_read_write.executeQueryToFrame(data_queries.get_state_query(state), conn)
            stats_calculator.calc_stats(state_price_data)

        print('Reading car type 1 query data: ')
        car_type_data = db_read_write.executeQueryToFrame(data_queries.get_car_type_query('%Ford%', None), conn)
        stats_calculator.calc_stats(car_type_data)

        print('Reading car type 2 query data: ')
        car_type_data = db_read_write.executeQueryToFrame(data_queries.get_car_type_query('%Chevy%',
                                                                                          '%Chevrolet%'), conn)
        stats_calculator.calc_stats(car_type_data)

        print('Reading car type 3 query data: ')
        car_type_data = db_read_write.executeQueryToFrame(data_queries.get_car_type_query('%Acura%', None), conn)
        stats_calculator.calc_stats(car_type_data)

        print('Reading car type 4 query data: ')
        car_type_data = db_read_write.executeQueryToFrame(data_queries.get_car_type_query('%Porsche%', None), conn)
        stats_calculator.calc_stats(car_type_data)

        print('Reading car type 5 query data: ')
        car_type_data = db_read_write.executeQueryToFrame(data_queries.get_car_type_query('%Lincoln%', None), conn)
        stats_calculator.calc_stats(car_type_data)

        print('Reading car type 6 query data: ')
        car_type_data = db_read_write.executeQueryToFrame(data_queries.get_car_type_query('%Dodge%', None), conn)
        stats_calculator.calc_stats(car_type_data)

        print('Reading car type 6 query data: ')
        car_type_data = db_read_write.executeQueryToFrame(data_queries.get_car_type_query('%Toyota%', None), conn)
        stats_calculator.calc_stats(car_type_data)

        print('Reading car type 6 query data: ')
        car_type_data = db_read_write.executeQueryToFrame(data_queries.get_car_type_query('%Rover%', None), conn)
        stats_calculator.calc_stats(car_type_data)

        print('Reading car type 7 query data: ')
        car_type_data = db_read_write.executeQueryToFrame(data_queries.get_car_type_query('%BMW%', None), conn)
        stats_calculator.calc_stats(car_type_data)

        print('Reading car type 8 query data: ')
        car_type_data = db_read_write.executeQueryToFrame(data_queries.get_car_type_query('%Nissan%', None), conn)
        stats_calculator.calc_stats(car_type_data)

        print('Reading car type 9 query data: ')
        car_type_data = db_read_write.executeQueryToFrame(data_queries.get_car_type_query('%Subaru%', None), conn)
        stats_calculator.calc_stats(car_type_data)


# --------------------------------------------------------------------
# python main idiom - so we can run from the command line as a script
if __name__ == "__main__":
    main()
