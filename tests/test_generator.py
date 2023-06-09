import pandas as pd
import yaml
from pathlib import Path

from src.generator import DataGenerator
from src.data_sources.kafka_data_source import KafkaDataSource

COLUMN_CONFIG_PATH = Path.cwd().parent / "src" / "config" / "column_config-test_synthetic_data.yml"


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
    kafka = KafkaDataSource('localhost:9092', 'coolcoolcool', 'test-pype', read_timeout_secs=10)
    num_partitions = num_processes = 3
    original_data = \
        {
            'first_name': ["Ran", "Yuval", "Ran", "John", "Mike", " Lenny", "Rojer", "Benny", "Dani", "Tomer"],
            'last_name': ["Dayan", "Mund", "Dayan", "Johnson", "Tyson", "Kravitz", "Smith", "Ben", "Dan", "Cohen"],
            'full_street': ["Hairus 5", "Herut 13", "Hairus 5", "Florentin 10", "Rotshild 11", "Summer 9",
                            "Alexander 8", "Levontin 5", "Levon 9", "Hairus 8"],
            'age': [28, 88, 28, 43, 46, 74, 45, 42, 48, 99]
        }

    data_frame = pd.DataFrame(original_data)
    for i in range(1000):
        kafka._write_df_to_data_source(data_frame, should_create_topic=i == 0, num_partitions=num_partitions)
    kafka.create_intermediate_data(
        num_processes=num_processes,
        is_synthetic=True,
        config_yml_path=Path.cwd().parent / "src" / "config" / "column_config-test_synthetic_data.yml"
    )
