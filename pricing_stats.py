import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# place holder for stats class
class pricing_stats_calculator:

    def calc_stats(self, commodity_price_data_frame):
        # for now, just print the frame
        print(commodity_price_data_frame.ASKING_PRICE.describe())
        plt.hist(commodity_price_data_frame['ASKING_PRICE'], bins = 20, range = [0.0, 25000])
        plt.show()
        plt.title('Price distribution')
        sns.distplot(commodity_price_data_frame['ASKING_PRICE'])
        plt.show()
