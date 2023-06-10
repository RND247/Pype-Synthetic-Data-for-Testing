import pandas as pd
import yaml

from generator import DataGenerator
from data_sources.kafka_data_source import KafkaDataSource

COLUMN_CONFIG_PATH = "../config/column_config-test_synthetic_data.yml"


def test_generate_synth_data_with_pii_columns():
    original_data = \
        {
            'first_name': ["Ran", "Yuval", "Ran", "John", "Mike"],
            'last_name': ["Dayan", "Mund", "Dayan", "Johnson", "Tyson"],
            'full_street': ["Hairus 5", "Herut 13", "Hairus 5", "Florentin 10", "Rotshild 11"],
            'age': [28, 88, 28, 43, 46]
        }
    with open(COLUMN_CONFIG_PATH, 'r') as file:
        column_config = yaml.safe_load(file)

    data_frame = pd.DataFrame(original_data)
    data_generator = DataGenerator(data_frame, column_config)
    synth_data = data_generator.generate_data()

    assert type(synth_data) == pd.DataFrame, "The generated synthetic data should be a Pandas DataFrame"
    assert synth_data.shape == data_frame.shape, "The shape of the synthetic data should match the original DataFrame"

    for column in synth_data.columns.tolist():
        if column_config[column]['is_pii']:
            for i in range(synth_data.shape[0]):
                assert synth_data[column].values[i] != data_frame[column][i], f"Expected different values," \
                                                                              f"got {data_frame[column][i]} in both tables"
        else:
            for i in range(synth_data.shape[0]):
                assert synth_data[column].values[i] == data_frame[column][i], f"Expected equal values," \
                                                                              f"got {synth_data[column].values[i]} and " \
                                                                              f"{data_frame[column][i]}"


def test_kafka_synth_data():
    kafka = KafkaDataSource('localhost:9092', 'my-topic-21', 'pype-yuval-test', read_timeout_secs=30)
    original_data = \
        {
            'first_name': ["Ran", "Yuval", "Ran", "John", "Mike"],
            'last_name': ["Dayan", "Mund", "Dayan", "Johnson", "Tyson"],
            'full_street': ["Hairus 5", "Herut 13", "Hairus 5", "Florentin 10", "Rotshild 11"],
            'age': [28, 88, 28, 43, 46]
        }

    data_frame = pd.DataFrame(original_data)
    for _ in range(1000):
        kafka._write_df_to_data_source(data_frame, should_create_topic=True)
    kafka.create_intermediate_data(
        num_processes=3,
        is_synthetic=True,
        config_yml_path=r"C:\Users\yuval\Documents\Dev\TDSD\config\column_config-test_synthetic_data.yml"
    )