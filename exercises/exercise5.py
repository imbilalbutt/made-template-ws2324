import zipfile as zf
from urllib.request import urlretrieve
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float

def validate_dataframe(df):
    df = df[df["zone_id"] == 2001]

    df = df[(df['stop_lat'] >= -90) & (df['stop_lat'] <= 90)]
    df = df[(df['stop_lon'] >= -90) & (df['stop_lon'] <= 90)]

    df = df.dropna(axis=0)

    return df

def establish_connection(database):
    engine = create_engine(f'sqlite:///{database}')
    metadata = MetaData()

    stops = Table('stops', metadata,
                  Column('stop_id', Integer, primary_key=True),
                  Column('stop_name', String),
                  Column('stop_lat', Float),
                  Column('stop_lon', Float),
                  Column('zone_id', Integer))

    metadata.create_all(engine)
    return engine

def load_data_in_database(engine, table, dataframe):
    connection = engine.connect()

    dataframe.to_sql(table, connection, if_exists='replace', index=False)

    connection.close()


if __name__ == '__main__':

    # Define constants
    _database = 'gtfs.sqlite'
    _table = 'stops'
    stops_filename = "stops.txt"
    zip_file_name = "GTFS.zip"

    # Download the zip file
    url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
    urlretrieve(url, zip_file_name)

    # Extract and get the exact file
    try:
        with zf.ZipFile(zip_file_name, 'r') as archive:
            # archive.extractall("./output_dir/")
            archive.extract(stops_filename, "./output_dir/")
    except zf.BadZipFile as error:
        print(error)

    stops_columns = ["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id"]

    stops_df = pd.read_csv("./output_dir/" + stops_filename, usecols=stops_columns, encoding='utf-8')

    # Filtering data
    stops_df = stops_df[stops_df["zone_id"] == 2001]

    stops_df = stops_df[(stops_df['stop_lat'] >= -90) & (stops_df['stop_lat'] <= 90)]
    stops_df = stops_df[(stops_df['stop_lon'] >= -90) & (stops_df['stop_lon'] <= 90)]

    stops_df = stops_df.dropna(axis=0)

    # stops_df = validate_dataframe(stops_df)

    engine = establish_connection(_database)
    load_data_in_database(engine, _table, stops_df)


