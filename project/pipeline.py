import pandas as pd
from sqlalchemy import Table, Column, create_engine, MetaData, String, TEXT, INTEGER, DECIMAL, FLOAT, BIGINT, inspect
import requests
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


class DataPipeline:

    def __init__(self, data_url, table, database):
        self.inspector = None
        self.data_url = data_url
        self.data = None
        self.connection = None
        self.table = table
        self.database = database
        self.engine = None
        self.cursor = None

    def download_data(self):
        try:
            #  Method 1: PERFECT -- works
            #  index_col = 0: makes the first Column as the indexer.
            self.data = pd.read_csv(self.data_url, sep=";", on_bad_lines='skip', header=0, index_col=0)

            # Method 2:
            # self.data = pd.read_csv(self.data_url, sep=";", on_bad_lines='skip', header=0)

            # self.data.columns.values.tolist() # To see the column names
            return self.data

        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
            return None
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
            return None
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
            return None
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)


    def transform_data(self):
        if self.table == 'water':
            # Method 1 (works):
            # In this method, indexing is already done (see download_data() method)
            # Select rows based on ranges: for rows [a:b] *(after comma)* for columns column [:m]
            # We are selecting rows based on integer locations
            # agriculture_df = self.data.iloc[0:6, :10] #
            # industry_df = self.data.iloc[6:12, :10]
            # chemicals_df = self.data.iloc[12:18, :10]
            # pharmaceuticals_df = self.data.iloc[18:24, :10]

            # Rename the columns
            # Example: Change from "Emissions to water, heavy metals/Mercury compounds like Hg (kg)"
            # to "Mercury"
            self.data.columns = ["Year", "Chromium", "Copper", "Mercury", "Lead", "Nickel", "Zinc", "OtherNutrients",
                                 "Phosphorus"]

            average = self.data["Mercury"].mean()
            # Explanation: First select all the rows of data of index 'A Agriculture, forestry and fishing':
            # using method loc(): loc['A Agriculture, forestry and fishing']
            # Then, select specific column of "Emissions to water, heavy metals/Mercury compounds like Hg (kg)"
            # ["Emissions to water, heavy metals/Mercury compounds like Hg (kg)"]
            self.data.at['A Agriculture, forestry and fishing', "Mercury"] = average

            # Method 2: Form an indexer and then split/select data accordingly
            #
            # Step 1: First make a particular column as an indexer using set_index()
            # here set_index() will works for whole dataset and treat column (header) named
            # ['Origin-destination'] as an indexer.
            # self.data.set_index(['Origin-destination'])

            # average = self.data["Emissions to water, heavy metals/Mercury compounds like Hg (kg)"].mean()
            # Step 2: To select particular rows of the dataset according to an indexer --> use loc
            # self.data.loc['A Agriculture, forestry and fishing']
            # self.data.at[
            #     'A Agriculture, forestry and fishing', "Emissions to water, heavy metals/Mercury compounds like Hg (kg)"] = average

        if self.table == 'vegetable':
            # Method 2: Form an indexer and then split/select data accordingly
            # Step 1: Set a column named 'Vegetables' as indexer
            # self.data.set_index(['Vegetables'])

            # Step 2: Select an indexer named 'Vegetables'
            # self.data.loc['Vegetables']

            # Rename the columns
            self.data.columns = ["year", "gross_yield_million_kilogram"]

    def establish_database_connection(self):
        self.engine = create_engine(f'sqlite:///{self.database}')
        metadata = MetaData()
        self.inspector = inspect(self.engine)

        # Define a table with column and data types
        if self.table == 'water':
            # Method 1: Deprecated --> works
            # if not self.engine.has_table(self.table):

            # Method 2: --> works
            if not self.inspector.has_table(self.table):
                water = Table(self.table, metadata,
                              Column('origin', TEXT),
                              Column('year', INTEGER),
                              Column('chromium', INTEGER),
                              Column('copper', INTEGER),
                              Column('mercury', INTEGER),
                              Column('lead', INTEGER),
                              Column('nickel', INTEGER),
                              Column('zinc', INTEGER),
                              Column('other_nutrients', BIGINT),
                              Column('phosphorus', INTEGER))

                # Create the table in the database
                metadata.create_all(self.engine)

        if self.table == 'vegetable':
            # Method 1: Deprecated --> works
            # if not self.engine.has_table(self.table):

            # Method 2: --> works
            if not self.inspector.has_table(self.table):
                vegetables = Table(self.table, metadata,
                                   Column('generic_vegetables', TEXT),
                                   Column('year', BIGINT),
                                   Column('gross_yield_million_kilogram', DECIMAL))

                # Create the table in the database
                metadata.create_all(self.engine)

    def load_data(self):
        self.connection = self.engine.connect()
        self.data.to_sql(self.table, self.connection, if_exists='replace', index=True)

        self.connection.close()

    def run_pipeline(self):

        if self.data_url is not None:
            data = self.download_data()

            if data is not None:
                self.transform_data()
                self.establish_database_connection()
                self.load_data()
                # self.get_all_data_from_database(self.table)

                print(f"ETL -- Successfully completed for {self.table}.")

            if data is None:
                return None
        else:
            raise FileNotFoundError(f'Failed to load from url {self.data_url}, Check if correct url is provided')

    def get_all_data_from_database(self, table_name):
        dataframe = pd.read_sql_table(table_name.lower(), self.get_connection())
        # print(dataframe)
        return dataframe

    def get_connection(self):

        # Method 1:
        # # Access the DB Engine
        self.engine = create_engine(f'sqlite:///{self.database}')
        self.connection = self.engine.connect()

        # Method 2: Does not work
        # con = sqlite3.connect("data/portal_mammals.sqlite")
        # self.connection = sqlite3.connect(self.database)

        return self.connection


if __name__ == '__main__':
    _database = '../data/data.sqlite'

    vegetable_table = 'vegetable'
    water_table = 'water'

    vegetable_data_url = "https://opendata.cbs.nl/CsvDownload/csv/37738ENG/TypedDataSet?dl=9ADD1"
    DataPipeline(data_url=vegetable_data_url, table=vegetable_table, database=_database).run_pipeline()

    water_data_url = "https://opendata.cbs.nl/CsvDownload/csv/83605ENG/TypedDataSet?dl=9ADCA"
    DataPipeline(data_url=water_data_url, table=water_table, database=_database).run_pipeline()

    # To query data from database
    DataPipeline(data_url=water_data_url, table=water_table, database=_database).get_all_data_from_database(table_name=water_table)
    DataPipeline(data_url=water_data_url, table=vegetable_table, database=_database).get_all_data_from_database(table_name=vegetable_table)
