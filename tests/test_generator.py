import pandas as pd

from column import FirstNameColumn, LastNameColumn, FullStreet, GeneralIntColumn
from generator import DataGenerator


def test_generate_synth_data_with_pii_columns():
    original_data = \
        {
            'fname': ["Ran", "Yuval", "Ran", "John", "Mike"],
            'lname': ["Dayan", "Mund", "Dayan", "Johnson", "Tyson"],
            'address': ["Hairus 5", "Herut 13", "Hairus 5", "Florentin 10", "Rotshild 11"],
            'age': [28, 88, 28, 43, 46]
        }
    columns_types_dict = \
        {
            'fname': FirstNameColumn,
            'lname': LastNameColumn,
            'address': FullStreet,
            'age': GeneralIntColumn
        }
    df = pd.DataFrame(original_data)
    pii_columns = ['fname', 'lname', 'address']

    dg = DataGenerator(df, columns_types_dict)
    synth_data = dg.generate_data(pii_columns)

    assert type(synth_data) == pd.DataFrame,  "The generated synthetic data should be a Pandas DataFrame"
    assert synth_data.shape == df.shape, "The shape of the synthetic data should match the original DataFrame"

    for column in synth_data.columns.tolist():
        if column in pii_columns:
            for i in range(synth_data.shape[0]):
                assert synth_data[column].values[i] != df[column][i], f"Expected different values," \
                                                                      f"got {df[column][i]} in both tables"
        else:
            for i in range(synth_data.shape[0]):
                assert synth_data[column].values[i] == df[column][i], f"Expected euqal values," \
                                                                      f"got {synth_data[column].values[i]} and " \
                                                                      f"{df[column][i]}"