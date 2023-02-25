import pandas as pd
import sqlite3 as sql




class dataManagement:
    
    def __init__(self, csv_path:str, db_path:str, name:str):
        
        """init metoden tar filvägar och namn och sparar det i variabler.

        Args:
            csv_path (str): filväg till csv
            db_path (str): vad databasen ska heta (.db)
            name (str): vad dataframe ska heta som omvandlas från csv
        """
        self.df = pd.read_csv(csv_path)
        self.db = sql.connect(db_path)
        self.name = name
        self.cur = self.db.cursor()

    def sep(self, colName, sepPrefix):
        """sep är en metod som sepererar ut alla olika vacciner
            ut till sina egna kolumner med egna kolumn namn.

        Args:
            colName (str): column namn som skall ändras
            sepPrefix (str): vilken prefix den skall seperera
        """
        self.colName = colName
        self.sepPrefix = sepPrefix
        split = self.df[self.colName].str.split(self.sepPrefix, expand=True)
        self.df = self.df.join(split)
        del self.df[self.colName]
        self.df.rename(columns={0 : "Vaccine_1", 1 : "Vaccine_2", 2 : "Vaccine_3", 3 : "Vaccine_4",
                                4 : "Vaccine_5", 5 : "Vaccine_6", 6 : "Vaccine_7"}, inplace = True)

    def database(self):
        """denna metod fyller den tomma databasen
        """
        try:
            self.df.to_sql(self.name, self.db)
        except Exception:
            pass

    #--------------------------------------------------------------------------------------------------------------------- VACCINES AND SOURCES

    def vac_and_source(self, tableName, insertFromTable):
        """denna metod skapar en tom tabell där den skapar alla kolumner i förväg 
            och vilken datatyp varje kolumn har och även vilken kolumn som är Primary key.

            skapar även en variabel 'tabletype' som har tilldelats "vacAndSourceTable" som
            är unik för metoden.

        Args:
            tableName (str): tar in ett namn för tabellen.
            insertFromTable (str): tar in namnet på tabellen som datan skall tas ifrån.
        """        
        self.tableName = tableName
        self.insertFromTable = insertFromTable
        tabletype = "vacAndSourceTable"

        try:
            self.cur.execute("PRAGMA FOREIGN_KEYS = ON")
            self.db.commit()
            self.cur.execute(f"""CREATE TABLE {tableName}(
                                country TEXT VARCHAR(255),
                                iso_code TEXT VARCHAR(10),
                                vaccine_1 TEXT VARCHAR(255),
                                vaccine_2 TEXT VARCHAR(255),
                                vaccine_3 TEXT VARCHAR(255),
                                vaccine_4 TEXT VARCHAR(255),
                                vaccine_5 TEXT VARCHAR(255),
                                vaccine_6 TEXT VARCHAR(255),
                                vaccine_7 TEXT VARCHAR(255),
                                source_name TEXT VARCHAR(255),
                                source_website TEXT VARCHAR(255),
                                PRIMARY KEY(country)
                                )
                            """)
            self.db.commit()
        except Exception:
            pass

        self.__insert_table(tableName, tabletype, insertFromTable)
        """kallar på den priviata metoden som stoppar in data i tomma tabellen som skapats.
        """        

    #--------------------------------------------------------------------------------------------------------------------- TOTAL VACCINATED
    
    def total_vac(self, tableName, insertFromTable):
        """denna metod skapar en tom tabell där den skapar alla kolumner i förväg 
            och vilken datatyp varje kolumn har och även vilken kolumn som är Primary key
            och vilken kolumn som är Foregin Key.

            skapar även en variabel 'tabletype' som har tilldelats "totalTable" som
            är unik för metoden.

        Args:
            tableName (str): tar in ett namn för tabellen.
            insertFromTable (str): tar in namnet på tabellen som datan skall tas ifrån.
        """         
        self.tableName = tableName
        self.insertFromTable = insertFromTable
        tabletype = "totalTable"

        try:
            self.cur.execute("PRAGMA FOREIGN_KEYS = ON")
            self.db.commit()
            self.cur.execute(f"""CREATE TABLE {tableName}(
                                country TEXT VARCHAR(255),
                                date DATE,
                                total_vaccinations REAL NULL,
                                total_vaccinations_per_hundred REAL NULL,
                                PRIMARY KEY(country),
                                FOREIGN KEY(country) REFERENCES vaccines_and_source(country)
                                )
                            """)
            self.db.commit()
        except Exception:
            pass
        
        self.__insert_table(tableName, tabletype, insertFromTable)
        """kallar på den priviata metoden som stoppar in data i tomma tabellen som skapats.
        """ 
    
    #--------------------------------------------------------------------------------------------------------------------- DAILY VACCINATED

    def daily_vac(self, tableName, insertFromTable):
        """denna metod skapar en tom tabell där den skapar alla kolumner i förväg 
            och vilken datatyp varje kolumn har och även vilka kolumner som är Primary key
            och vilken kolumn som är Foregin Key.

            skapar även en variabel 'tabletype' som har tilldelats "dailyTable" som
            är unik för metoden.

        Args:
            tableName (str): tar in ett namn för tabellen.
            insertFromTable (str): tar in namnet på tabellen som datan skall tas ifrån.
        """               
        self.tableName = tableName
        self.insertFromTable = insertFromTable
        tabletype = "dailyTable"

        try:
            self.cur.execute("PRAGMA FOREIGN_KEYS = ON")
            self.db.commit()
            self.cur.execute(f"""CREATE TABLE {tableName}(
                                country TEXT VARCHAR(255),
                                date DATE,
                                daily_vaccinations REAL NULL,
                                PRIMARY KEY(country, date),
                                FOREIGN KEY(country) REFERENCES vaccines_and_source(country)
                                )
                            """)
            self.db.commit()
        except Exception:
            pass
        
        self.__insert_table(tableName, tabletype, insertFromTable)
        """kallar på den priviata metoden som stoppar in data i tomma tabellen som skapats.
        """ 

    #--------------------------------------------------------------------------------------------------------------------- PEOPLE VACCINATED
    
    def people_vac(self, tableName, insertFromTable):
        """denna metod skapar en tom tabell där den skapar alla kolumner i förväg 
            och vilken datatyp varje kolumn har och även vilken kolumn som är Primary key
            och vilken kolumn som är Foregin Key.

            skapar även en variabel 'tabletype' som har tilldelats "peopleTable" som
            är unik för metoden.

        Args:
            tableName (str): tar in ett namn för tabellen.
            insertFromTable (str): tar in namnet på tabellen som datan skall tas ifrån.
        """        
        self.tableName = tableName
        self.insertFromTable = insertFromTable
        tabletype = "peopleTable"

        try:
            self.cur.execute("PRAGMA FOREIGN_KEYS = ON")
            self.db.commit()
            self.cur.execute(f"""CREATE TABLE {tableName}(
                                country TEXT VARCHAR(255),
                                date DATE,
                                people_vaccinated REAL NULL,
                                people_vaccinated_per_hundred REAL NULL,
                                people_fully_vaccinated REAL NULL,
                                people_fully_vaccinated_per_hundred REAL NULL,
                                PRIMARY KEY(country),
                                FOREIGN KEY(country) REFERENCES vaccines_and_source(country)
                                )
                            """)
            self.db.commit()
        except Exception:
            pass

        self.__insert_table(tableName, tabletype, insertFromTable)
        """kallar på den priviata metoden som stoppar in data i tomma tabellen som skapats.
        """ 

    #--------------------------------------------------------------------------------------------------------------------- FILLING NULL TO 0 OR "NO VALUE"

    def fill_na_values(self):
        """denna metod ersätter alla NULL värden med 0 eller "No Value" berode på om
            kolumnen innehåller floats eller strings.
        """        
        values = {"total_vaccinations": 0, "people_vaccinated": 0, "people_fully_vaccinated": 0,
                    "daily_vaccinations_raw": 0, "daily_vaccinations": 0, "total_vaccinations_per_hundred": 0,
                    "people_vaccinated_per_hundred": 0, "people_fully_vaccinated_per_hundred": 0, "daily_vaccinations_per_million": 0}

        values2 = {"Vaccine_1": "No Value", "Vaccine_2": "No Value", "Vaccine_3": "No Value",
                    "Vaccine_4": "No Value", "Vaccine_5": "No Value", "Vaccine_6": "No Value", "Vaccine_7": "No Value"}
            
        self.df = self.df.fillna(value=values)
        self.df = self.df.fillna(value=values2)
            
    #--------------------------------------------------------------------------------------------------------------------- PRIVATE METHOD - INSERT INTO TABLE

    def __insert_table(self, tableName, tabletype, insertFromTable):
        """denna metod tilldelar data till de tomma tabellerna som skapats.
            if-statsen koller från vilken metod tabellen skapas genom att se
            vilken string som är tilldelad till 'tabletype' och utifårn det
            så tilldelar metoden data till den specifika tabellen.

        Args:
            tableName (str): tar in ett namn för tabellen.
            tabletype (str): tar emot en unik string som tilldelades i metoden.
            insertFromTable (str): tar in namnet på tabellen som datan skall tas ifrån.
        """        
        self.tableName = tableName
        self.tabletype = tabletype
        self.insertFromTable = insertFromTable
        
        if(tabletype == "vacAndSourceTable"):
            try:
                self.cur.execute(f"""INSERT INTO {tableName}
                                SELECT country,
                                    iso_code,
                                    vaccine_1,
                                    vaccine_2,
                                    vaccine_3,
                                    vaccine_4,
                                    vaccine_5,
                                    vaccine_6,
                                    vaccine_7,
                                    source_name,
                                    source_website
                                FROM {insertFromTable}
                                GROUP BY country
                                """)
                self.db.commit()
            except Exception:
                    pass

        elif(tabletype == "totalTable"):
            try:
                self.cur.execute(f"""INSERT INTO {tableName}
                                SELECT country,
                                    date,
                                    MAX(total_vaccinations) AS total_vaccinations,
                                    MAX(total_vaccinations_per_hundred) AS total_vaccinations_per_hundred
                                FROM {insertFromTable}
                                WHERE total_vaccinations NOT NULL
                                AND total_vaccinations_per_hundred NOT NULL
                                GROUP BY country
                                """)
                self.db.commit()
            except Exception:
                    pass

        elif(tabletype == "dailyTable"):
            try:
                self.cur.execute(f"""INSERT INTO {tableName}
                                SELECT country,
                                    date,
                                    daily_vaccinations
                                FROM {insertFromTable}
                                WHERE daily_vaccinations NOT NULL
                                """)
                self.db.commit()
            except Exception:
                    pass
        
        elif(tabletype == "peopleTable"):
            try:
                self.cur.execute(f"""INSERT INTO {tableName}
                                SELECT country,
                                    date,
                                    MAX(people_vaccinated) AS people_vaccinated,
                                    MAX(people_vaccinated_per_hundred) AS people_vaccinated_per_hundred,
                                    MAX(people_fully_vaccinated) AS people_fully_vaccinated,
                                    MAX(people_fully_vaccinated_per_hundred) AS people_fully_vaccinated_per_hundred
                                FROM {insertFromTable}
                                
                                GROUP BY country
                                """)
                self.db.commit()
            except Exception:
                    pass


