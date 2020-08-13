
![Image of Yaktocat](https://cdn.iconscout.com/icon/free/png-256/craigslist-283553.png)
![Image of Yaktocat](https://freeiconshop.com/wp-content/uploads/edd/car-glyph-side-view.png)


# Craigslist Car Listing Scraper

-Python program that scrapes all car listings from all 400+ USA craigslist links, with price, nehgborhood, state, and listing title info.

-Ported from jupyter notebook into class structure, able to write to Microsoft SQL server for scald data management 

-craigslistUSA.xlsx contains all Craigslist car listing ads in USA, can edit to add/remove any listings

## How To Use
-Toggle boolean run switches in main.py to choose if you want to run analysis, fetch new results, read to database, or do all. For instance: 

  run_fetch_price_data = False
  run_test_queries = False
  run_test_read_write = False
  run_calc_stats = True


will only run statistical analysis on collected data, whereas 

  run_fetch_price_data = True
  run_test_queries = False
  run_test_read_write = False
  run_calc_stats = True
  
will fetch new data from live Craigslist listings and *then* run statistical analysis 
