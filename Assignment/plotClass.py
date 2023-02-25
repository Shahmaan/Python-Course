import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


class plotter:
    def __init__(self, db_path):
        """denna tar emot databas fil väg och skapar en anslutning.

        Args:
            db_path (str): fil sök vägen till databasen
        """        
        self.db_path = sqlite3.connect(db_path)

    def __query(self):
        """denna privata metod skapar en dataframe fom en SQL query och sedan sparar det i variablen balkan.
        """        
        self.balkan = pd.read_sql("""SELECT country, date, daily_vaccinations
                    FROM daily_vac
                    WHERE date >= '2021-03-01' AND date <= '2021-03-31' AND country = 'Albania' 
                    OR country = 'Serbia' AND date >= '2021-03-01' AND date <= '2021-03-31'
                    OR country = 'Slovenia' AND date >= '2021-03-01' AND date <= '2021-03-31'
                    OR country = 'Bosnia and Herzegovina' AND date >= '2021-03-01' AND date <= '2021-03-31'
                    OR country = 'Montenegro' AND date >= '2021-03-01' AND date <= '2021-03-31'
                    OR country = 'Bulgaria' AND date >= '2021-03-01' AND date <= '2021-03-31'
                    OR country = 'Greece' AND date >= '2021-03-01' AND date <= '2021-03-31'
                    OR country = 'Croatia' AND date >= '2021-03-01' AND date <= '2021-03-31'
                    OR country = 'North Macedonia' AND date >= '2021-03-01' AND date <= '2021-03-31'
                    OR country = 'Romania' AND date >= '2021-03-01' AND date <= '2021-03-31'
                """, self.db_path)
        return self.balkan
    
    def plotting(self):
        """denna metod tar in balkan datafrme och skapar en plot.
        """        
        self.balkanNew = self.__query()
        sns.lineplot(x='date', y='daily_vaccinations', data=self.balkan, hue="country")
        plt.show()