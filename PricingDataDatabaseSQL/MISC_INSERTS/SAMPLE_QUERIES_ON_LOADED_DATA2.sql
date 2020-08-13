-- Some test base sql queries, without state filters
-- added junk filter.
-- Next pass, remove duplicates same state, same price, same title
select ASKING_PRICE, STATE_ID, TITLE, NEIGHBORHOOD from COMMODITY_PRICE_INFO
where ASKING_PRICE NOT IN (111111, 121212, 123456, 134531, 99999) AND
TITLE NOT LIKE '%junk%' AND
TITLE NOT LIKE '%wanted%' AND
TITLE NOT LIKE '%WTB%' AND
TITLE NOT LIKE '%TOP CASH%' AND
TITLE NOT LIKE '%LOOKING FOR%' AND
TITLE NOT LIKE '%We Buy%' AND
TITLE NOT LIKE '%rent%' AND
TITLE NOT LIKE '%lease%' AND
TITLE NOT LIKE '%CASH FOR CARS%' 
order by ASKING_PRICE DESC


select AVG(ASKING_PRICE), MIN(ASKING_PRICE), MAX(ASKING_PRICE) from COMMODITY_PRICE_INFO
where ASKING_PRICE NOT IN (111111, 121212, 123456, 134531, 99999) AND
TITLE NOT LIKE '%junk%' and
TITLE NOT LIKE '%wanted%' AND
TITLE NOT LIKE '%WTB%' AND
TITLE NOT LIKE '%TOP CASH%' AND
TITLE NOT LIKE '%LOOKING FOR%' AND
TITLE NOT LIKE '%We Buy%' 

-- delete from COMMODITY_PRICE_INFO;







-- Some test base sql queries, without state filters
-- added junk filter.
-- Next pass, remove duplicates same state, same price, same title
select STATE_ID, COUNT(STATE_ID), AVG(ASKING_PRICE), MIN(ASKING_PRICE), MAX(ASKING_PRICE)  from COMMODITY_PRICE_INFO
where ASKING_PRICE NOT IN (111111, 121212, 123456, 134531, 99999) AND
TITLE NOT LIKE '%junk%' AND
TITLE NOT LIKE '%wanted%' AND
TITLE NOT LIKE '%WTB%' AND
TITLE NOT LIKE '%TOP CASH%' AND
TITLE NOT LIKE '%LOOKING FOR%' AND
TITLE NOT LIKE '%We Buy%' AND
TITLE NOT LIKE '%rent%' AND
TITLE NOT LIKE '%lease%' AND
TITLE NOT LIKE '%CASH FOR CARS%' 
GROUP BY STATE_ID
order by AVG(ASKING_PRICE) DESC


-- Some test base sql queries, without state filters
-- added junk filter.
-- Next pass, remove duplicates same state, same price, same title
select STATE_ID, COUNT(STATE_ID), AVG(ASKING_PRICE), MIN(ASKING_PRICE), MAX(ASKING_PRICE)  from COMMODITY_PRICE_INFO
where ASKING_PRICE NOT IN (111111, 121212, 123456, 134531, 99999) AND
TITLE NOT LIKE '%junk%' AND
TITLE NOT LIKE '%wanted%' AND
TITLE NOT LIKE '%WTB%' AND
TITLE NOT LIKE '%TOP CASH%' AND
TITLE NOT LIKE '%LOOKING FOR%' AND
TITLE NOT LIKE '%We Buy%' AND
TITLE NOT LIKE '%rent%' AND
TITLE NOT LIKE '%lease%' AND
TITLE NOT LIKE '%CASH FOR CARS%' 
GROUP BY STATE_ID
order by COUNT(STATE_ID) DESC

-- Some test base sql queries, without state filters
-- added junk filter.
-- Next pass, remove duplicates same state, same price, same title
select STATE_ID, COUNT(STATE_ID), AVG(ASKING_PRICE), MIN(ASKING_PRICE), MAX(ASKING_PRICE)  from COMMODITY_PRICE_INFO
where ASKING_PRICE NOT IN (111111, 121212, 123456, 134531, 99999) AND
TITLE NOT LIKE '%junk%' AND
TITLE NOT LIKE '%wanted%' AND
TITLE NOT LIKE '%WTB%' AND
TITLE NOT LIKE '%TOP CASH%' AND
TITLE NOT LIKE '%LOOKING FOR%' AND
TITLE NOT LIKE '%We Buy%' AND
TITLE NOT LIKE '%rent%' AND
TITLE NOT LIKE '%lease%' AND
TITLE NOT LIKE '%CASH FOR CARS%' 
GROUP BY STATE_ID
order by MAX(ASKING_PRICE) DESC


-- Some test base sql queries, without state filters
-- added junk filter.
-- Next pass, remove duplicates same state, same price, same title
select ASKING_PRICE, STATE_ID, TITLE from COMMODITY_PRICE_INFO
where ASKING_PRICE NOT IN (111111, 121212, 123456, 134531, 99999) AND
TITLE NOT LIKE '%junk%' AND
TITLE NOT LIKE '%wanted%' AND
TITLE NOT LIKE '%WTB%' AND
TITLE NOT LIKE '%TOP CASH%' AND
TITLE NOT LIKE '%LOOKING FOR%' AND
TITLE NOT LIKE '%We Buy%' AND
TITLE NOT LIKE '%rent%' AND
TITLE NOT LIKE '%lease%' AND
TITLE NOT LIKE '%CASH FOR CARS%' AND
TITLE LIKE '%Mustang%'
order by ASKING_PRICE DESC