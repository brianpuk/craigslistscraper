
![Image of Yaktocat](https://cdn.iconscout.com/icon/free/png-256/craigslist-283553.png)
![Image of Yaktocat](https://freeiconshop.com/wp-content/uploads/edd/car-glyph-side-view.png)


# Craigslist Car Listing Scraper

-Python program that scrapes and analyzes all car listings from all 400+ USA craigslist links, with price, nehgborhood, state, and listing title info

-Can write to Microsoft SQL Server using PyODBC for scaled data management 

-```craigslistUSA.xlsx``` contains all Craigslist listing link bases in USA, can edit to add/remove any listings from any particular areas

## How To Use
-Toggle boolean run switches in ```main.py``` to choose if you want to run analysis, fetch new results, read to database, or do all. For instance: 

 ```
  run_fetch_price_data = False
  run_test_queries = False
  run_test_read_write = False
  run_calc_stats = True
```



will only run statistical analysis on collected data, whereas 


 ```
  run_fetch_price_data = True
  run_test_queries = False
  run_test_read_write = False
  run_calc_stats = True
```
  
will fetch new data from live Craigslist listings and *then* run statistical analysis 

Also be sure to change ```self.server``` in ```db_readwrite.py``` to **your** machine's name

 ```
def __init__(self):
        self.server = "" <--repalce with your machine's name
        self.database = "SALE_PRICE_STATS"
        self.userName = "TestUser"
        self.passWord = "TestPassword"
```

## SQL

-Can use pre written SQL scripts in ```PricingDataDatabaseSQL``` to initialize databases in your local machine for state data like census regions, GDP, etc.

- run ```001__SQL_DB_AND_TABLE_INSERTS```, then ```002__SQL_STATE_INFO_INSERTS```

