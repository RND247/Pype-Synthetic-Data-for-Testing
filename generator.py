import pandas as pd

from column import NAME_FUNC_DICT  # Wraparound
from utils.shared_memory_manager import SharedMemoryManager


class DataGenerator:
    data: pd.DataFrame
    shared_memory: SharedMemoryManager

    def __init__(self, data: pd.DataFrame, column_config, shared_memory=None):
        self.data = data
        self.schema = self._create_data_schema()
        self.shared_memory = shared_memory
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
        memory_val = self.shared_memory.get_value(org_val)
        if memory_val:
            return memory_val
        else:
            module = NAME_FUNC_DICT[column['module']]
            instance = module(**column)
            new_val = instance.generate_value()
            self.shared_memory.set_value(org_val, new_val)
        return new_val
