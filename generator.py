import pandas as pd

from column import BaseColumn


class DataGenerator:
    data: pd.DataFrame

    def __init__(self, data, columns_types_dict):
        self.data = data
        self.schema = self._create_data_schema()
        self.tracker_dict = {}
        self.columns_types_dict = columns_types_dict

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
                generated_data[column] = [self.generate_by_category(self.columns_types_dict[column],
                                                                    self.data[column].values[i])
                                          for i in range(num_of_rows)]
            else:
                # Take the existing value of the column
                generated_data[column] = self.data[column].values
        return pd.DataFrame(generated_data)

    def generate_by_category(self, column: BaseColumn, org_val):
        if org_val in self.tracker_dict:
            return self.tracker_dict[org_val]
        else:
            self.tracker_dict[org_val] = column().generate_value()
        return self.tracker_dict[org_val]