from pipeline import DataPipeline
import unittest


class TestFullyConnected2(unittest.TestCase):

    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.water_table = None
        self.water_data_url = None
        self._database = None
        self.vegetable_table = None
        self.vegetable_data_url = None

    def setUp(self):
        _database = '../data/data.sqlite'

        self.vegetable_table = 'vegetable'
        self.water_table = 'water'

        self.vegetable_data_url = "https://opendata.cbs.nl/CsvDownload/csv/37738ENG/TypedDataSet?dl=9ADD1"
        self.water_data_url = "https://opendata.cbs.nl/CsvDownload/csv/83605ENG/TypedDataSet?dl=9ADCA"


    def test_get_connection(self):

        dp_veg = DataPipeline(data_url=self.vegetable_data_url, table=self.vegetable_table, database=self._database)
        dp_veg.run_pipeline()

        database_connection = dp_veg.get_connection()

        # Check if the connection with the database is successful.
        self.assertTrue(database_connection)


    def test_pipeline(self):

        dp_veg = DataPipeline(data_url=self.vegetable_data_url, table=self.vegetable_table, database=self._database)
        dp_veg.run_pipeline()

        # Check if data was loaded in database and is retrievable
        data_veg = dp_veg.get_all_data_from_database(self.water_table)

        # Check if data was loaded in database and is retrievable
        self.assertIsNotNone(data_veg)

        dp_water = DataPipeline(data_url=self.water_data_url, table=self.water_table, database=self._database)
        dp_water.run_pipeline()

        # Check if data was loaded in database and is retrievable
        data_water = dp_water.get_all_data_from_database(self.water_table)
        self.assertIsNotNone(data_water)


if __name__ == '__main__':
    unittest.main()