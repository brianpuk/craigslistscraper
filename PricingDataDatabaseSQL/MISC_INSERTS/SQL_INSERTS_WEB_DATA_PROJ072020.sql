
-- This will drop and re-create the database and tables
-- Comment out the drop database if you don't want to lose data.

USE master;
GO
IF DB_ID (N'SALE_PRICE_STATS') IS NOT NULL
DROP DATABASE SALE_PRICE_STATS;
GO
CREATE DATABASE SALE_PRICE_STATS;
GO

USE SALE_PRICE_STATS;
-- State info table, base table to identify state, state name, population, gdp
CREATE TABLE STATE_INFO (
    STATE_ID VARCHAR(2) NOT NULL, -- Two character state abbrev. Key.  AZ
    STATE_NAME VARCHAR (30) NOT NULL,-- Full state name, current accepted. E.g. California, Massachusetts.

    -- Population data, absolute, rank and percent.
    POP DECIMAL(12,0) NOT NULL, -- Asof 2019, latest.
    POP_RANK DECIMAL(12,0) NOT NULL, -- Asof 2019, latest.
    POP_PERCENT DECIMAL(12,0) NOT NULL, -- Asof 2019, latest.

    -- GDP data, absolute dollars, rank and percent.
    GDP_DOLLARS DECIMAL(15,0) NOT NULL, -- Asof 2019, latest.
    GDP_RANK  DECIMAL(15,0) NOT NULL, -- Asof 2019, latest.
    GDP_PERCENT DECIMAL(15,0) NOT NULL, -- Asof 2019, latest.
    GDP_PER_CAPITA DECIMAL(15,0) NOT NULL, -- Asof 2019, latest.

    -- Region subkey, either fed, or census
    REGION_SK_FED VARCHAR(25) NOT NULL,-- This is the region SUBKEY, e.g. Federal region, 'South West'
    REGION_SK_DIST VARCHAR(25) NOT NULL,-- This is the region SUBKEY, e.g. Census region, similar to above but different classification	

    LAST_UPD_DATE DATETIME -- The last time we updated this table.
);



-- Keep this for example for other tables.
-- FOREIGN KEY (STATE_ID) REFERENCES SALES_PRICE_STATS.STATE_INFO (STATE_ID)


-- For the car data, we don't always know the city, but do know state, so also region.
-- Some craigslist sites are specified by geographical area rather than specific city.
-- This is the root table for our raw price data scanned from the web.
CREATE TABLE COMMODITY_PRICE_INFO (
    STATE_ID VARCHAR(2) NOT NULL, -- Two character state abbrev. Key.  AZ.  For e.g. craigslist data, we get from locator.
    AD_SOURCE_INFO_ID_NAME VARCHAR(30), -- where we got the data. For the first version, will be craigslist. Later, can add others. 
    COMMODITY VARCHAR(30), -- What the item is, car, etc.   For first version, will be only vehicles for sale. Later, can be sep table.	
    ASKING_PRICE  DECIMAL(13,0) NOT NULL, -- The advertisements asking price. We don't know the sales price. Use bigger field so we can do, e.g. houses, or apartments.
    TITLE 	VARCHAR(30), -- Descriptive title. 
    NEIGHBORHOOD VARCHAR(30), -- Additional locator data.     
    SCAN_DATE DATETIME, -- This is when we ran this scan.  Not the ad date, which we might not know.
);


-- What is the best way to split off add source?  How we scan depends on the site.  
-- So, this url format and how its interpreted is different for each site.  
-- This record will need to tell the loader what site parser to use. 
-- E.g. CraigslistAdLoaderParser
-- For e.g. craigslist, there are lots of independent web sites.
-- For others, probably not, e.g. only one Cars.com.
-- So, we'll need to fetch, we may get 
CREATE TABLE AD_SOURCE_INFO (
    -- Since Craisglist has multiple sites, we need a 2 key field.
    -- For those sites that have one web site, but you can search by area, the region id will be ROOT.
    AD_SOURCE_INFO_ID_NAME VARCHAR(20) NOT NULL, -- For example, Craigslist, Cars.com, AutoTrader, Carmax, Truecar.com
    AD_SOURCE_INFO_LINK  VARCHAR(256) NOT NULL, -- This is the URL we use to get to the particular site.  May be multiple
    AD_SOURCE_INFO_STATE_ID VARCHAR(4) NOT NULL, -- This is the locator for the site, if it is not localized, then ROOT which means there is one site, but can search by region inside the site.
);

-- Can hard code the python library for the first release.  
CREATE TABLE AD_SOURCE_INFO_BY_NAME(
    AD_SOURCE_INFO_ID_NAME VARCHAR(30) NOT NULL, -- Craigslist, CarMax, AutoTrader, Truecar, etc.  
    
    SOURCE_INFO_PARSER VARCHAR(30) NOT NULL, -- This tells the engine what parser to use. It corresponds to a Python library.
);









