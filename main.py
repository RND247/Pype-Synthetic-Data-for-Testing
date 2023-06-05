from utils.logging_config import configure_logger
from generator import DataGenerator
import pandas as pd

configure_logger()

data = {'id': ['12345678', '12344567', '12345678'], 'name': ["Ran", "Yuval", "Ran"], 'age': [27, 56, 25]}
df = pd.DataFrame(data)

pii_columns = ["id", "name"]

dg = DataGenerator(df)
synth_data = dg.generate_data(pii_columns)

print(synth_data)