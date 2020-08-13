

USE SALE_PRICE_STATS;
drop table COMMODITY_PRICE_INFO;

-- For the car data, we don't always know the city, but do know state, so also region.
-- Some craigslist sites are specified by geographical area rather than specific city.
-- This is the root table for our raw price data scanned from the web.
CREATE TABLE COMMODITY_PRICE_INFO (
    STATE_ID VARCHAR(2) NOT NULL, -- Two character state abbrev. Key.  AZ.  For e.g. craigslist data, we get from locator.
    AD_SOURCE_INFO_ID_NAME VARCHAR(30), -- where we got the data. For the first version, will be craigslist. Later, can add others. 
    COMMODITY VARCHAR(30), -- What the item is, car, etc.   For first version, will be only vehicles for sale. Later, can be sep table.	
    ASKING_PRICE  DECIMAL(13,0) NOT NULL, -- The advertisements asking price. We don't know the sales price. Use bigger field so we can do, e.g. houses, or apartments.
    TITLE 	VARCHAR(512), -- Descriptive title. 
    NEIGHBORHOOD VARCHAR(120), -- Additional locator data.     
    SCAN_DATE DATETIME, -- This is when we ran this scan.  Not the ad date, which we might not know.
);