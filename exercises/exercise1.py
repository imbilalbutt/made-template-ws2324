import pandas as pd
from sqlalchemy import Table, Column, create_engine, MetaData, String, TEXT, INTEGER, Float, DECIMAL


class DataPipeline:

    def __init__(self, data_url, table, database):
        self.data_url = data_url
        self.data = None
        self.connection = None
        self.table = table  # 'airports'
        self.database = database  # 'airports.sqlite'
        self.engine = None

    def download_data(self):
        self.data = pd.read_csv(self.data_url, sep=";", on_bad_lines='skip')
        return self.data

    def establish_connection(self):
        # You can create the file with touch my_data.db
        # Path('my_data.db').touch()

        self.engine = create_engine(f'sqlite:///{self.database}')
        metadata = MetaData()

        # self.connection = sqlite3.connect('my_data.db')
        # self.cursor = self.connection.cursor()
        # self.cursor.execute('''CREATE TABLE users (user_id int, username text)''')

        # Define a table with column and data types
        airports = Table(self.table, metadata,
                         Column('column_1', INTEGER),
                         Column('column_2', String),
                         Column('column_3', String),
                         Column('column_4', String),
                         Column('column_5', String),
                         Column('column_6', String),
                         Column('column_7', Float),
                         Column('column_8', Float),
                         Column('column_9', INTEGER),
                         Column('column_10',Float),
                         Column('column_11', TEXT),
                         Column('column_12', String),
                         Column('geo_punkt', DECIMAL))

        # Create the table in the database
        metadata.create_all(self.engine)

        # return self.cursor

    def load_data(self):
        # Method 1: Works
        # df.to_sql(self.table, f'sqlite:///{self.database}', if_exists='replace', index=False, dtype={})

        self.connection = self.engine.connect()
        self.data.to_sql(self.table, self.connection, if_exists='replace', index=False)

        self.connection.close()

    def run_pipeline(self):
        print("self.data = ", self.data_url)
        if self.data_url is not None:
            self.download_data()
            self.establish_connection()
            self.load_data()
        else:
            raise FileNotFoundError(f'Failed to load from url {self.data_url}, Check if correct url is provided')
        print("Success!")


if __name__ == '__main__':
    _data_url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"

    _table = 'airports'
    _database = 'airports.sqlite'

    dp = DataPipeline(data_url=_data_url, table=_table, database=_database).run_pipeline()
