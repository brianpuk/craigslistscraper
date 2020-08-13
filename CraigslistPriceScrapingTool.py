import pandas as pd
import numpy as np
import requests
import re  # regex builtins
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
import time  # For time sleep to pause between patch runs
import lxml  # Need to install
from db_readwrite import DbReadWrite
import datetime


# Class that loads links for all Craiglist sites in the USA, runs the links to get pricing for cars for sale
# The output of this class can be saved to e.g. SQL Server or other database.
# Usage:  ak_df = mergeStateDF('AK', ak_df, usa_df)
class CraigslistScraper:
    # in seconds to avoid fail to connect.
    between_query_pause = 5

    stateAbbrev = []
    state_data_frames = {}

    searchPopStart = "search/cto?"
    searchPopEnd = "query=CARS&sort=rel&min_price=100&max_price=500000"
    craigsListLinkFile = '''D:\projecys\craigslist\craigslistUSA.xlsx'''

    listing_column_names = ['TITLE', 'ASKING_PRICE', 'NEIGHBORHOOD', 'STATE_ID', 'AD_SOURCE_INFO_ID_NAME', 'COMMODITY',
                            'SCAN_DATE']

    stateAbbrev = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
                   'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
                   'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
                   'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'QA', 'RI', 'SC', 'SD', 'TN',
                   'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']

    def get_state_abbrev(self):
        return self.stateAbbrev



    # constructor sets up array of hard coded states, and data frame for each to hold
    # cl data.
    def __init__(self):
        # First, create the state data frames
        # array of data frames, one for each state.
        for stateItem in self.stateAbbrev:
            print('Creating state data frame: ', stateItem)
            self.state_data_frames[stateItem] = pd.DataFrame()

    # end init --------------------------------------------

    # internal
    # get the portion of the url that defines what we're searching for and the page to get.
    # page seems to have 120 ads but we don't rely on that.
    # Link looks like: https://auburn.craigslist.org/search/cto?s=120&query=CARS&sort=rel&min_price=100&max_price=500000
    # where s=120 means to start at item 120
    def get_search_pop(self, start_point):
        if start_point == None:
            return self.searchPopStart + self.searchPopEnd
        else:
            return self.searchPopStart + "s=" + format(start_point) + "&" + self.searchPopEnd

    # end get_search_pop --------------------------------------------

    # This method reads rows from the spreadsheet that contains roughly 400 links to
    # craigslist sites all over the USA, one row per.  Rows are grouped by state.
    # This reads those rows, and appends a specific link for CARS for sale.
    # Later, we can extend this class to search for other commodities.
    def get_usa_data_frame(self):
        # This is the part of the search link to append to every state link.
        usa_df = pd.read_excel(open(self.craigsListLinkFile, 'rb'), sheet_name='Sheet1')
        usa_df['link'] = usa_df['link'].astype(str)  # + self.searchPop
        return usa_df

    # end get_usa_data_frame --------------------------------------------

    # Create data frame for one link.  This is one craigslist site,e.g. Craigslist, Oneonta.
    # Output a data frame containing price data for items for sale.
    # This is the workhorse function that loads the page data, parses, and generates the data we want in a dataframe
    def createDF(self, link, state_name):
        count_ads = 0
        listingInfo = []
        full_link = link + self.get_search_pop(None)

        # Run the first query. This gets up to about 120 items.
        # We don't know how many total, so we set the start item
        # on the query, using s=NNN and run again until we don't get
        # any results back.
        try:
            response = requests.get(full_link)
        except ConnectionError as e:  # This is the correct syntax
            print("Connection failed: ", e)
            return pd.DataFrame(columns=self.listing_column_names)

        soup = BeautifulSoup(response.text, 'lxml')
        carcontent = soup.findAll('p', {'class': 'result-info'})

        current_count = len(carcontent)
        print("Fetched " + format(current_count) + " ads in current pass for state " +
              state_name + " current_count: " + format(count_ads) + " link: " + full_link)

        # While loop fetches the next page, if any.
        # For small states/sites, there may be only one page.
        while current_count > 0:
            for car in carcontent:
                listing = {}

                # Fetch the actual ad data
                listing['TITLE'] = car.find("a", {"class": "result-title hdrlnk"}).text
                listing['ASKING_PRICE'] = int(car.find("span", {'class': 'result-price'}).text.replace('$', ''))
                hood = car.find('span', {"class": 'result-hood'})
                listing['NEIGHBORHOOD'] = hood.text.replace('(', '').replace(')', '') if hood else None

                # meta data
                listing['STATE_ID'] = state_name
                listing['AD_SOURCE_INFO_ID_NAME'] = "Craigslist"
                listing['COMMODITY'] = 'CARS'
                listing['SCAN_DATE'] = None
                listingInfo.append(listing)

            # We use arbitrary 120
            count_ads = count_ads + min(120, current_count)
            full_link = link + self.get_search_pop(count_ads)

            # wait some time before fetching the next page, try to avoid timeout.
            time.sleep(self.between_query_pause)

            # now run the query again, but start at higher start count
            # set s=NNN where NNN is which posting number to start at.
            try:
                response = requests.get(full_link)
            except ConnectionError as e:
                print("Connection failed: ", e)
                return pd.DataFrame(columns=self.listing_column_names)

            soup = BeautifulSoup(response.text, 'lxml')
            carcontent = soup.findAll('p', {'class': 'result-info'})

            # How many items did we get in this fetch?
            # If it is >0, then go to the while loop process this batch and run again.
            current_count = len(carcontent)
            print("Fetched " + format(current_count) + " ads in current pass for state " +
                  state_name + " current_count: " + format(count_ads) + " link: " + full_link)

            # end for ---------
        # end while ----------

        listingPage = pd.DataFrame(listingInfo)
        return (listingPage)

    # end createDF --------------------------------------------

    # This is the development version, that brings back the complete first page for each site link
    # The only change here from dev version is to get the piece of the URL that could have the start flag
    # The start flag is not used here, since its the first page.
    def createDF_ONEPAGE(self, link, state_name):
        response = requests.get(link) + self.get_search_pop(None)
        soup = BeautifulSoup(response.text, 'lxml')
        carcontent = soup.findAll('p', {'class': 'result-info'})
        listingInfo = []
        for car in carcontent:
            listing = {}
            listing['TITLE'] = car.find("a", {"class": "result-title hdrlnk"}).text
            listing['ASKING_PRICE'] = int(car.find("span", {'class': 'result-price'}).text.replace('$', ''))
            hood = car.find('span', {"class": 'result-hood'})
            listing['NEIGHBORHOOD'] = hood.text.replace('(', '').replace(')', '') if hood else None

            listing['STATE_ID'] = state_name
            listing['AD_SOURCE_INFO_ID_NAME'] = "Craigslist"
            listing['COMMODITY'] = 'CARS'
            listing['SCAN_DATE'] = None

            listingInfo.append(listing)
        listingPage = pd.DataFrame(listingInfo)
        return (listingPage)

    # Top level function, given a state name. i.e. 'AK'
    # find all links for that state in the master list
    # then, merge.
    def mergeStateDF(self, state_name, state_df, usa_df):
        for index, row in usa_df.iterrows():
            if (row['state'] == state_name):
                # Here, use createDF_ONEPAGE to use the development version (one page)
                temp_df = self.createDF(row['link'], state_name)
                state_df = pd.concat([state_df, temp_df])
                temp_df = temp_df.iloc[0:0]
        return (state_df)

    # end mergeStateDF --------------------------------------------

    # Run a subset of states, and return the processed data frame list
    # The caller will save, pause, then start another batch.
    def runBatch(self, state_sub_list, usa_df):
        # Run the set, then give back a list of dataframes we processed.
        # This is to reduce the total list that we process at once.
        processed_data_frames = {}
        for state_abbrev in state_sub_list:
            self.state_data_frames[state_abbrev] = self.mergeStateDF(state_abbrev,
                                                                     self.state_data_frames[state_abbrev],
                                                                     usa_df)
            processed_data_frames[state_abbrev] = self.state_data_frames[state_abbrev]
        return processed_data_frames

    # end runBatch --------------------------------------------

    def processOneBatch(self, dbReadWrite, usa_df, state_sub_list, currDateTimeString, save_to_database):
        processed_data_frames = self.runBatch(state_sub_list, usa_df)
        for key in processed_data_frames:
            print(key, '->', processed_data_frames[key])
            if (save_to_database == True):
                dbReadWrite.writeCommodityPriceInfo(processed_data_frames[key], currDateTimeString)
        time.sleep(self.between_query_pause)

    # end processOneBatch --------------------------------------------

    # Run all states by sub batch.
    def run_all_states_by_batch(self, save_to_database):
        start_time = time.time()

        currDateTime = datetime.datetime.now()
        currDateTimeString = currDateTime.strftime('%x')

        usa_df = self.get_usa_data_frame()
        dbReadWrite = DbReadWrite()

        self.processOneBatch(dbReadWrite, usa_df, ['AK', 'AL', 'AR', 'AZ'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['CA'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['CO', 'CT', 'DC', 'DE', 'FL'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['GA', 'HI', 'IA', 'ID'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['IL', 'IN', 'KS', 'KY'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['LA', 'MA', 'MD'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['ME', 'MI', 'MN', 'MO'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['MS', 'MT', 'NC', 'ND', 'NE'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['NH', 'NJ', 'NM', 'NV'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['NY', 'OH', 'OK'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['OR', 'PA', 'QA', 'RI'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['SC', 'SD', 'TN'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['TX', 'UT'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['VA', 'VT', 'WA'], currDateTimeString, save_to_database)

        self.processOneBatch(dbReadWrite, usa_df, ['WI', 'WV', 'WY'], currDateTimeString, save_to_database)

        print("The current run took %s seconds." % (time.time() - start_time))

    # end run_all_states_by_batch --------------------------------------------

    # Original method runs well in Jupyter during development fo rphase 1.
    # Save for testing.
    def run_all_states(self, save_to_db):

        start_time = time.time()

        usa_df = self.get_usa_data_frame()

        ak_df = pd.DataFrame()
        al_df = pd.DataFrame()
        ar_df = pd.DataFrame()
        az_df = pd.DataFrame()
        ca_df = pd.DataFrame()
        co_df = pd.DataFrame()
        ct_df = pd.DataFrame()
        dc_df = pd.DataFrame()
        de_df = pd.DataFrame()
        fl_df = pd.DataFrame()
        ga_df = pd.DataFrame()
        hi_df = pd.DataFrame()
        ia_df = pd.DataFrame()
        id_df = pd.DataFrame()
        il_df = pd.DataFrame()
        in_df = pd.DataFrame()
        ks_df = pd.DataFrame()
        ky_df = pd.DataFrame()
        la_df = pd.DataFrame()
        ma_df = pd.DataFrame()
        md_df = pd.DataFrame()
        me_df = pd.DataFrame()
        mi_df = pd.DataFrame()
        mn_df = pd.DataFrame()
        mo_df = pd.DataFrame()
        ms_df = pd.DataFrame()
        mt_df = pd.DataFrame()
        nc_df = pd.DataFrame()
        nd_df = pd.DataFrame()
        ne_df = pd.DataFrame()
        nh_df = pd.DataFrame()
        nj_df = pd.DataFrame()
        nm_df = pd.DataFrame()
        nv_df = pd.DataFrame()
        ny_df = pd.DataFrame()
        oh_df = pd.DataFrame()
        ok_df = pd.DataFrame()
        or_df = pd.DataFrame()
        pa_df = pd.DataFrame()
        qa_df = pd.DataFrame()
        ri_df = pd.DataFrame()
        sc_df = pd.DataFrame()
        sd_df = pd.DataFrame()
        tn_df = pd.DataFrame()
        tx_df = pd.DataFrame()
        ut_df = pd.DataFrame()
        va_df = pd.DataFrame()
        vt_df = pd.DataFrame()
        wa_df = pd.DataFrame()
        wi_df = pd.DataFrame()
        wv_df = pd.DataFrame()
        wy_df = pd.DataFrame()

        # Write a few frames to the database see if it works
        dbReadWrite = DbReadWrite()
        currDateTime = datetime.datetime.now()
        currDateTimeString = currDateTime.strftime('%x')

        ak_df = self.mergeStateDF('AK', ak_df, usa_df)
        al_df = self.mergeStateDF('AL', al_df, usa_df)
        ar_df = self.mergeStateDF('AR', ar_df, usa_df)
        az_df = self.mergeStateDF('AZ', az_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(ak_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(al_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(ar_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(az_df, currDateTimeString)
        time.sleep(self.between_query_pause)
        print('AK', ' ', ak_df)
        print('AL', ' ', al_df)
        print('AR', ' ', ar_df)
        print('AZ', ' ', az_df)
        # stop and write frames to database.
        if (save_to_db == True):
            ca_df = self.mergeStateDF('CA', ca_df, usa_df)
        print('CA', ' ', ca_df)
        time.sleep(self.between_query_pause)

        co_df = self.mergeStateDF('CO', co_df, usa_df)
        ct_df = self.mergeStateDF('CT', ct_df, usa_df)
        dc_df = self.mergeStateDF('DC', dc_df, usa_df)
        de_df = self.mergeStateDF('DE', de_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(co_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(ct_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(dc_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(de_df, currDateTimeString)
        print('CO', ' ', co_df)
        print('CT', ' ', ct_df)
        print('DC', ' ', dc_df)
        print('DE', ' ', de_df)
        time.sleep(self.between_query_pause)

        fl_df = self.mergeStateDF('FL', fl_df, usa_df)
        ga_df = self.mergeStateDF('GA', ga_df, usa_df)
        hi_df = self.mergeStateDF('HI', hi_df, usa_df)
        ia_df = self.mergeStateDF('IA', ia_df, usa_df)
        id_df = self.mergeStateDF('ID', id_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(fl_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(ga_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(hi_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(ia_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(id_df, currDateTimeString)
        print('FL', ' ', fl_df)
        print('GA', ' ', ga_df)
        print('HI', ' ', hi_df)
        print('IA', ' ', ia_df)
        print('ID', ' ', id_df)
        time.sleep(self.between_query_pause)

        il_df = self.mergeStateDF('IL', il_df, usa_df)
        in_df = self.mergeStateDF('IN', in_df, usa_df)
        ks_df = self.mergeStateDF('KS', ks_df, usa_df)
        ky_df = self.mergeStateDF('KY', ky_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(il_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(in_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(ks_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(ky_df, currDateTimeString)

        print('IL', ' ', il_df)
        print('IN', ' ', in_df)
        print('KS', ' ', ks_df)
        print('KY', ' ', ky_df)
        time.sleep(self.between_query_pause)

        la_df = self.mergeStateDF('LA', la_df, usa_df)
        ma_df = self.mergeStateDF('MA', ma_df, usa_df)
        md_df = self.mergeStateDF('MD', md_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(la_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(ma_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(md_df, currDateTimeString)

        print('LA', ' ', la_df)
        print('MA', ' ', ma_df)
        print('MD', ' ', md_df)
        time.sleep(self.between_query_pause)

        me_df = self.mergeStateDF('ME', me_df, usa_df)
        mi_df = self.mergeStateDF('MI', mi_df, usa_df)
        mn_df = self.mergeStateDF('MN', mn_df, usa_df)
        mo_df = self.mergeStateDF('MO', mo_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(me_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(mi_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(mn_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(mo_df, currDateTimeString)

        print('ME', ' ', me_df)
        print('MI', ' ', mi_df)
        print('MN', ' ', mn_df)
        print('MO', ' ', mo_df)
        time.sleep(self.between_query_pause)

        ms_df = self.mergeStateDF('MS', ms_df, usa_df)
        mt_df = self.mergeStateDF('MT', mt_df, usa_df)
        nc_df = self.mergeStateDF('NC', nc_df, usa_df)
        nd_df = self.mergeStateDF('ND', nd_df, usa_df)
        ne_df = self.mergeStateDF('NE', ne_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(ms_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(mt_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(nc_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(nd_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(ne_df, currDateTimeString)

        print('MS', ' ', ms_df)
        print('MT', ' ', mt_df)
        print('NC', ' ', nc_df)
        print('ND', ' ', nd_df)
        print('NE', ' ', ne_df)
        time.sleep(self.between_query_pause)

        nh_df = self.mergeStateDF('NH', nh_df, usa_df)
        nj_df = self.mergeStateDF('NJ', nj_df, usa_df)
        nm_df = self.mergeStateDF('NM', nm_df, usa_df)
        nv_df = self.mergeStateDF('NV', nv_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(nh_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(nj_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(nm_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(nv_df, currDateTimeString)

        print('NH', ' ', nh_df)
        print('NJ', ' ', nj_df)
        print('NM', ' ', nm_df)
        print('NV', ' ', nv_df)
        time.sleep(self.between_query_pause)

        ny_df = self.mergeStateDF('NY', ny_df, usa_df)
        oh_df = self.mergeStateDF('OH', oh_df, usa_df)
        ok_df = self.mergeStateDF('OK', ok_df, usa_df)
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(ny_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(oh_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(ok_df, currDateTimeString)

        # stop and write frames to database.
        print('NY', ' ', ny_df)
        print('OH', ' ', oh_df)
        print('OK', ' ', ok_df)
        time.sleep(self.between_query_pause)

        or_df = self.mergeStateDF('OR', or_df, usa_df)
        pa_df = self.mergeStateDF('PA', pa_df, usa_df)
        qa_df = self.mergeStateDF('QA', qa_df, usa_df)
        ri_df = self.mergeStateDF('RI', ri_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(or_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(pa_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(qa_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(ri_df, currDateTimeString)

        print('OR', ' ', or_df)
        print('PA', ' ', pa_df)
        print('QA', ' ', qa_df)
        print('RI', ' ', ri_df)
        time.sleep(self.between_query_pause)

        sc_df = self.mergeStateDF('SC', sc_df, usa_df)
        sd_df = self.mergeStateDF('SD', sd_df, usa_df)
        tn_df = self.mergeStateDF('TN', tn_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(sc_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(sd_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(tn_df, currDateTimeString)

        print('SC', ' ', sc_df)
        print('SD', ' ', sd_df)
        print('TN', ' ', tn_df)
        time.sleep(self.between_query_pause)

        tx_df = self.mergeStateDF('TX', tx_df, usa_df)
        ut_df = self.mergeStateDF('UT', ut_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(tx_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(ut_df, currDateTimeString)

        print('TX', ' ', tx_df)
        print('UT', ' ', ut_df)
        time.sleep(self.between_query_pause)

        va_df = self.mergeStateDF('VA', va_df, usa_df)
        vt_df = self.mergeStateDF('VT', vt_df, usa_df)
        wa_df = self.mergeStateDF('WA', wa_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(va_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(vt_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(wa_df, currDateTimeString)

        print('VA', ' ', va_df)
        print('VT', ' ', vt_df)
        print('WA', ' ', wa_df)
        time.sleep(self.between_query_pause)

        wi_df = self.mergeStateDF('WI', wi_df, usa_df)
        wv_df = self.mergeStateDF('WV', wv_df, usa_df)
        wy_df = self.mergeStateDF('WY', wy_df, usa_df)
        # stop and write frames to database.
        if (save_to_db == True):
            dbReadWrite.writeCommodityPriceInfo(wi_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(wv_df, currDateTimeString)
            dbReadWrite.writeCommodityPriceInfo(wy_df, currDateTimeString)

        print('WI', ' ', wi_df)
        print('WV', ' ', wv_df)
        print('WY', ' ', wy_df)
        time.sleep(self.between_query_pause)

        print("The current run took %s seconds." % (time.time() - start_time))
    # end run_all_states --------------------------------------------
