import pandas as pd
from sqlalchemy import Table, Column, create_engine, MetaData, String, TEXT, INTEGER, DECIMAL, FLOAT, BIGINT
import csv
import requests
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


class DataPipeline:

    def __init__(self, data_url, table, database):
        self.data_url = data_url
        self.data = None
        self.connection = None
        self.table = table
        self.database = database
        self.engine = None

    def download_data(self):
        try:
            self.data = pd.read_csv(self.data_url, sep=";", on_bad_lines='skip', header=0)
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
        return self.data

    def download_data_v2(self):
        with requests.Session() as s:
            download = s.get(self.data_url)

            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)

            # Not tested below
            # from io import StringIO
            # text = StringIO(download.content.decode('utf-8'))
            # df = pd.read_csv(text)

            for row in my_list:
                print(row)

    def transform_data(self):
        if self.table == 'water':
            # agriculture_df = df.iloc[0:6, :10]
            # industry_df = df.iloc[6:12, :10]
            # chemicals_df = df.iloc[12:18, :10]
            # pharmaceuticals_df = df.iloc[18:24, :10]
            pass

        if self.table == 'vegetable':
            pass

    def establish_database_connection(self):
        self.engine = create_engine(f'sqlite:///{self.database}')
        metadata = MetaData()

        # Define a table with column and data types
        if self.table == 'water':
            water = Table(self.table, metadata,
                               Column('origin', TEXT),
                               Column('year', INTEGER),
                               Column('chromic', INTEGER),
                               Column('copper', INTEGER),
                               Column('mercury', INTEGER),
                               Column('lead', INTEGER),
                               Column('nickel', INTEGER),
                               Column('zinc', INTEGER),
                               Column('total_nutrients', BIGINT),
                               Column('phosphorus', INTEGER))

        if self.table == 'vegetable':
            vegetables = Table(self.table, metadata,
                               Column('generic_vegetables', TEXT),
                               Column('period', BIGINT),
                               Column('gross_yield_million_kilogram', DECIMAL))

        # Create the table in the database
        if self.table is not None:
            metadata.create_all(self.engine)

    def load_data(self):
        self.connection = self.engine.connect()
        self.data.to_sql(self.table, self.connection, if_exists='replace', index=False)

        self.connection.close()

    def run_pipeline(self):

        if self.data_url is not None:
            self.download_data()
            self.transform_data()
            self.establish_database_connection()
            self.load_data()
        else:
            raise FileNotFoundError(f'Failed to load from url {self.data_url}, Check if correct url is provided')
        print(f"ETL -- Successfully completed for {self.table}.")


if __name__ == '__main__':
    _database = '../data/data.sqlite'
    vegetable_table = 'vegetable'
    water_table = 'water'

    # vegetable_data_url_v2 = "https://opendata.cbs.nl/CsvDownload/csv/37738ENG/TypedDataSet?dl=90C91"
    vegetable_data_url_v3 = "https://opendata.cbs.nl/CsvDownload/csv/37738ENG/TypedDataSet?dl=9ADD1" # "https://opendata.cbs.nl/CsvDownload/csv/37738ENG/TypedDataSet?dl=9ADCE"

    vegetable_dp = DataPipeline(data_url=vegetable_data_url_v3, table=vegetable_table, database=_database).run_pipeline()

    # water_data_url = "https://opendata.cbs.nl/statline/#/CBS/en/dataset/83605ENG/table?ts=1698675109480"
    water_data_url_v2 = "https://opendata.cbs.nl/CsvDownload/csv/83605ENG/TypedDataSet?dl=9ADCA"
    # "https://opendata.cbs.nl/CsvDownload/csv/83605ENG/TypedDataSet?dl=13C85"

    water_dp = DataPipeline(data_url=water_data_url_v2, table=water_table, database=_database).run_pipeline()
