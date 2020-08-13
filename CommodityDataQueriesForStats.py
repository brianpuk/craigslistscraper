# queries sql, to fetch subsets of data from the main data set.
class commodity_data_queries:
    region_1 = '''Region_1_Northeast'''
    region_2 = '''Region_2_Midwest'''
    region_3 = '''Region_3_South'''
    region_4 = '''Region_4_West'''

    # strings need to be quoted.
    region_queryLHS = '''select STATE_ID, AD_SOURCE_INFO_ID_NAME, COMMODITY, ASKING_PRICE, TITLE, NEIGHBORHOOD, SCAN_DATE
    from COMMODITY_PRICE_INFO where STATE_ID IN ( select STATE_ID from STATE_INFO where REGION_SK_FED = \''''
    region_queryRHS = '''\');'''

    # query all commodity_price_info data for region 1.
    def get_region_1_query(self):
        return self.region_queryLHS + self.region_1 + self.region_queryRHS

    # query all commodity_price_info data for region 1.
    def get_region_2_query(self):
        return self.region_queryLHS + self.region_2 + self.region_queryRHS

    # query all commodity_price_info data for region 1.
    def get_region_3_query(self):
        return self.region_queryLHS + self.region_3 + self.region_queryRHS

    # query all commodity_price_info data for region 1.
    def get_region_4_query(self):
        return self.region_queryLHS + self.region_4 + self.region_queryRHS

    # state id is two char abbreviation, e.g. 'CA', or 'NV'
    def get_state_query(self, state_id):
        return '''select  STATE_ID, AD_SOURCE_INFO_ID_NAME, COMMODITY, ASKING_PRICE, TITLE, NEIGHBORHOOD, SCAN_DATE
        from COMMODITY_PRICE_INFO where STATE_ID IN(\'''' + state_id + '''\');'''

    # Append to this, the rank part of the query.
    base_rank_query = '''select C.STATE_ID, AD_SOURCE_INFO_ID_NAME, COMMODITY, ASKING_PRICE, TITLE, NEIGHBORHOOD, 
                            SCAN_DATE, I.POP, I.GDP_DOLLARS from COMMODITY_PRICE_INFO C
                            join STATE_INFO I on C.STATE_ID = I.STATE_ID '''

    pop_rank_predicates = ['''where I.POP_RANK >= 1 AND I.POP_RANK <=5;''',
                           '''where I.POP_RANK >= 6 AND I.POP_RANK <=10;''',
                           '''where I.POP_RANK >= 11 AND I.POP_RANK <=15;''',
                           '''where I.POP_RANK >= 16 AND I.POP_RANK <=20;''',
                           '''where I.POP_RANK >= 21 AND I.POP_RANK <=25;''',
                           '''where I.POP_RANK >= 26 AND I.POP_RANK <=30;''',
                           '''where I.POP_RANK >= 31 AND I.POP_RANK <=35;''',
                           '''where I.POP_RANK >= 36 AND I.POP_RANK <=40;''',
                           '''where I.POP_RANK >= 41 AND I.POP_RANK <=45;''',
                           '''where I.POP_RANK >= 46 AND I.POP_RANK <=51;''']

    # -- Can do by pop rank.  1-5, 6-10, 11-15, 16-20, 21-25, 26-30, 31-35, 36-40, 41-45, 46-51
    def get_pop_rank_query(self, pop_rank):
        if isinstance(pop_rank, int) == False:
            raise Exception('The argument pop_rank should be an integer, with value 1 to 10')
        else:
            if (pop_rank < 1 or pop_rank > 10):
                raise Exception('THe argument pop_rank value between 1 and 10 inclusive')
        return self.base_rank_query + self.pop_rank_predicates[pop_rank - 1]

    gdp_rank_predicates = ['''where I.GDP_RANK >= 1 AND I.GDP_RANK <=5;''',
                           '''where I.GDP_RANK >= 6 AND I.GDP_RANK <=10;''',
                           '''where I.GDP_RANK >= 11 AND I.GDP_RANK <=15;''',
                           '''where I.GDP_RANK >= 16 AND I.GDP_RANK <=20;''',
                           '''where I.GDP_RANK >= 21 AND I.GDP_RANK <=25;''',
                           '''where I.GDP_RANK >= 26 AND I.GDP_RANK <=30;''',
                           '''where I.GDP_RANK >= 31 AND I.GDP_RANK <=35;''',
                           '''where I.GDP_RANK >= 36 AND I.GDP_RANK <=40;''',
                           '''where I.GDP_RANK >= 41 AND I.GDP_RANK <=45;''',
                           '''where I.GDP_RANK >= 46 AND I.GDP_RANK <=51;''']

    # -- Can do by gdp rank.  1-5, 6-10, 11-15, 16-20, 21-25, 26-30, 31-35, 36-40, 41-45, 46-51
    def get_gdp_rank_query(self, gdp_rank):
        if isinstance(gdp_rank, int) == False:
            raise Exception('The argument gdp_rank should be an integer, with value 1 to 10')
        else:
            if (gdp_rank < 1 or gdp_rank > 10):
                raise Exception('The argument gdp_rank value between 1 and 10 inclusive')
        return self.base_rank_query + self.gdp_rank_predicates[gdp_rank - 1]


    # return records for a car type could be make or model, like Ford or Corvette or Land Rover
    title_query_lhs = '''select C.STATE_ID, AD_SOURCE_INFO_ID_NAME, COMMODITY, ASKING_PRICE, TITLE, 
                NEIGHBORHOOD, SCAN_DATE, I.POP, I.GDP_DOLLARS
                from COMMODITY_PRICE_INFO  C  join STATE_INFO  I  on C.STATE_ID = I.STATE_ID
                where  TITLE like '''
    def get_car_type_query(self, title_like_clause1, title_like_clause2):
        query = self.title_query_lhs + '\'' + title_like_clause1 + '\''
        if(title_like_clause2 != None):
            query = query + ' or TITLE LIKE \'' + title_like_clause2 + '\''
        return query





