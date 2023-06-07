import pandas as pd

from column import NAME_FUNC_DICT  # Wraparound


class DataGenerator:
    data: pd.DataFrame

    def __init__(self, data, column_config):
        self.data = data
        self.schema = self._create_data_schema()
        self.tracker_dict = {}
        self.column_config = column_config

    def _create_data_schema(self):
        column_names = self.data.columns.tolist()
        schema = {key: set() for key in column_names}
        for row in self.data.iloc():
            for key in column_names:
                schema[key].add(str(type(row[key])))

        return schema

    def generate_data(self) -> pd.DataFrame:
        num_of_rows = self.data.shape[0]
        generated_data = {}
        for column in self.data.columns.tolist():
            if self.column_config[column]['is_pii']:
                # Randomize a value for the column
                generated_data[column] = [self.generate_by_category(self.column_config[column],
                                                                    self.data[column].values[i])
                                          for i in range(num_of_rows)]
            else:
                # Take the existing value of the column
                generated_data[column] = self.data[column].values
        return pd.DataFrame(generated_data)

    def generate_by_category(self, column: dict[str], org_val):
        if org_val in self.tracker_dict:
            return self.tracker_dict[org_val]
        else:
            module = NAME_FUNC_DICT[column['module']]
            instance = module(**column)
            self.tracker_dict[org_val] = instance.generate_value()
        return self.tracker_dict[org_val]
