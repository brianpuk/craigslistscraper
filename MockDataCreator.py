import pyodbc
import pandas as pd


# Create mock data frames to test db read write
class MockDataCreator:

    def __init__(self):
        self.columns = ['STATE_ID', 'AD_SOURCE_INFO_ID_NAME', 'COMMODITY', 'ASKING_PRICE', 'TITLE', 'NEIGHBORHOOD',
                   'SCAN_DATE']

    # tester method to get mock commodity price info data.
    # Use for testing inserts.
    def getMockCommodityPriceInfoDataFrame(self):

        df = pd.DataFrame(None, None, self.columns)
        # Inefficient row by row add, but we'll get a frame from the web scraping tool.
        modifiedDataFrame = df.append({'STATE_ID': 'NY',
                                       'AD_SOURCE_INFO_ID_NAME': 'Craigslist NYC',
                                       'COMMODITY': 'Cars For Sale',
                                       'ASKING_PRICE': 13229.4,
                                       'TITLE': '1986 Red Car',
                                       'NEIGHBORHOOD': 'NY Manhattan',
                                       'SCAN_DATE': '08/06/2020'
                                       }, ignore_index=True)

        modifiedDataFrame = modifiedDataFrame.append({'STATE_ID': 'CA',
                                                      'AD_SOURCE_INFO_ID_NAME': 'Craigslist Los Angeles',
                                                      'COMMODITY': 'Cars For Sale',
                                                      'ASKING_PRICE': 23139.4,
                                                      'TITLE': '2015 Green Car',
                                                      'NEIGHBORHOOD': 'CA LA',
                                                      'SCAN_DATE': '08/06/2020'
                                                      }, ignore_index=True)

        modifiedDataFrame = modifiedDataFrame.append({'STATE_ID': 'TX',
                                                      'AD_SOURCE_INFO_ID_NAME': 'Craigslist Austin',
                                                      'COMMODITY': 'Cars For Sale',
                                                      'ASKING_PRICE': 44101.4,
                                                      'TITLE': '2019 Orange Car',
                                                      'NEIGHBORHOOD': 'CA TX',
                                                      'SCAN_DATE': '08/06/2020'
                                                      }, ignore_index=True)

        modifiedDataFrame = modifiedDataFrame.append({'STATE_ID': 'TX',
                                                      'AD_SOURCE_INFO_ID_NAME': 'Craigslist Austin',
                                                      'COMMODITY': 'Cars For Sale',
                                                      'ASKING_PRICE': 44101.4,
                                                      'TITLE': '2019 Orange Car',
                                                      'NEIGHBORHOOD': 'CA TX',
                                                      'SCAN_DATE': '08/06/2020'
                                                      }, ignore_index=True)
        return modifiedDataFrame
        # End of getMockCommodityPriceInfoDataFrame
        # -----------------------------------------------------------------------------
