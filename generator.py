import pandas as pd
from faker import Faker

tracker_dict = {}
ID = ["id"]
FULL_NAME = ["name", "full name", "full_name"]
FIRST_NAME = ["first name", "first_name", "fname"]
LAST_NAME = ["last name", "last_name", "surname", "lname"]


class DataGenerator:
    data: pd.DataFrame

    def __init__(self, data):
        self.data = data
        self.fake = Faker()
        self.schema = self._create_data_schema()

    def _create_data_schema(self):
        column_names = self.data.columns.tolist()
        schema = {key: set() for key in column_names}
        for row in self.data.iloc():
            for key in column_names:
                schema[key].add(str(type(row[key])))

        return schema

    def generate_data(self, pii_columns: list[str]) -> pd.DataFrame:
        num_of_rows = self.data.shape[0]
        generated_data = {}
        for column in self.data.columns.tolist():
            if column in pii_columns:
                # Randomize a value for the column
                generated_data[column] = [self.generate_by_category(column, self.data[column].values[i])
                                          for i in range(num_of_rows)]
            else:
                # Take the existing value of the column
                generated_data[column] = self.data[column].values
        return pd.DataFrame(generated_data)

    def generate_by_category(self, column: str, org_val):
        if org_val in tracker_dict:
            return tracker_dict[org_val]
        elif column.lower() in ID:
            tracker_dict[org_val] = str(self.fake.pyint(0,999999999))
        elif column.lower() in FULL_NAME:
            tracker_dict[org_val] = self.fake.name()
        elif column.lower() in FIRST_NAME:
            tracker_dict[org_val] = self.fake.first_name()
        elif column.lower() in LAST_NAME:
            tracker_dict[org_val] = self.fake.last_name()
        return tracker_dict[org_val]