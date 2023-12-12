import pandas as pd
from sqlalchemy import Table, Column, create_engine, MetaData, String, TEXT, INTEGER, FLOAT


class DataPipeline:

    def __init__(self, data_url, table, database):
        self.data_url = data_url
        self.data = None
        self.connection = None
        self.table = table  # 'cars'
        self.database = database  # 'cars.sqlite'
        self.engine = None

    def download_data_from_url(self):
        self.data = pd.read_csv(self.data_url, sep=";", on_bad_lines='skip', skiprows=7, skipfooter=4, engine='python', header=None, encoding="ISO-8859-1")


        return self.data

    def transform_validate_data(self):
        self.data = self.data.iloc[:, [0, 1, 2, 12, 22, 32, 42, 52, 62, 72]]


        self.data.rename(
            columns={0: 'date', 1: 'CIN', 2: 'name', 12: 'petrol', 22: 'diesel', 32: 'gas', 42: 'electro', 52: 'hybrid',
                     62: 'plugInHybrid', 72: 'others'}, inplace=True)

        # filter out whitespaces
        filter_petrol = self.data["petrol"] != ""
        self.data = self.data[filter_petrol]

        filter_diesel = self.data["diesel"] != ""
        self.data = self.data[filter_diesel]

        filter_gas = self.data["gas"] != ""
        self.data = self.data[filter_gas]

        filter_electro = self.data["electro"] != ""
        self.data = self.data[filter_electro]

        filter_hybrid = self.data["hybrid"] != ""
        self.data = self.data[filter_hybrid]

        filter_plugInHybrid = self.data["plugInHybrid"] != ""
        self.data = self.data[filter_plugInHybrid]

        filter_others = self.data["others"] != ""
        self.data = self.data[filter_others]

        # filter out for dashes
        filter_petrol = self.data["petrol"] != "-"
        self.data = self.data[filter_petrol]

        filter_diesel = self.data["diesel"] != "-"
        self.data = self.data[filter_diesel]

        filter_gas = self.data["gas"] != "-"
        self.data = self.data[filter_gas]

        filter_electro = self.data["electro"] != "-"
        self.data = self.data[filter_electro]

        filter_hybrid = self.data["hybrid"] != "-"
        self.data = self.data[filter_hybrid]

        filter_plugInHybrid = self.data["plugInHybrid"] != "-"
        self.data = self.data[filter_plugInHybrid]

        filter_others = self.data["others"] != "-"
        self.data = self.data[filter_others]

        self.data = self.data.astype(
            {'date': str, 'CIN': str, 'name': str, 'petrol': int, 'diesel': int, 'gas': int, 'electro': int,
             'hybrid': int, 'plugInHybrid': int, 'others': int})

        self.data['CIN'] = self.data['CIN'].astype(str).str.zfill(5)

        self.data['petrol'] = self.data['petrol'].clip(lower=0)
        self.data['diesel'] = self.data['diesel'].clip(lower=0)
        self.data['gas'] = self.data['gas'].clip(lower=0)
        self.data['electro'] = self.data['electro'].clip(lower=0)
        self.data['hybrid'] = self.data['hybrid'].clip(lower=0)
        self.data['plugInHybrid'] = self.data['plugInHybrid'].clip(lower=0)
        self.data['others'] = self.data['others'].clip(lower=0)

        self.data.dropna(inplace=True)

    def establish_connection_to_database(self):

        self.engine = create_engine(f'sqlite:///{self.database}')
        metadata = MetaData()

        # Define a table with column and data types
        cars = Table('cars', metadata,
                     Column('date', String),
                     Column('CIN', String(length=5)),
                     Column('name', String),
                     Column('petrol', INTEGER),
                     Column('diesel', INTEGER),
                     Column('gas', INTEGER),
                     Column('electro', INTEGER),
                     Column('hybrid', INTEGER),
                     Column('plugInHybrid', INTEGER),
                     Column('others', INTEGER),
                     )

        # Create the table in the database
        metadata.create_all(self.engine)

        # return self.cursor

    def load_data_in_database(self):

        self.connection = self.engine.connect()
        self.data.to_sql(self.table, self.connection, if_exists='replace', index=False)
        self.connection.close()

    def run_pipeline(self):
        if self.data_url is not None:
            self.download_data_from_url()
            self.transform_validate_data()
            self.establish_connection_to_database()
            self.load_data_in_database()
        else:
            raise FileNotFoundError(f'Failed to load from url {self.data_url}, Check if correct url is provided')
        print("ETL -- Successfully completed.")


if __name__ == '__main__':
    _data_url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"

    _table = 'cars'
    _database = 'cars.sqlite'

    dp = DataPipeline(data_url=_data_url, table=_table, database=_database).run_pipeline()
