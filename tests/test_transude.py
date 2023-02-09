import pandas as pd
import transude as txd
import unittest
from datetime import datetime

from transude import DataFrameFilter, DataFrameFilterManager


class TestTransude(unittest.TestCase):
    def setUp(self):
        # Create mock dataframes for testing
        self.pd_df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "color": ["blue", "red", "green", "red", "blue"],
                "size": ["small", "medium", "large", "small", "medium"],
            }
        )

    def test_filter_pandas(self):
        # Test filtering a Pandas DataFrame using Transude
        filtered_df = txd.filter_df(data_frame=self.pd_df, columns='color', values='red', operator='==')
        self.assertEqual(2, filtered_df.shape[0])
        self.assertEqual({'red'}, set(filtered_df['color']))

        filtered_df = txd.filter_df(data_frame=self.pd_df, columns='id', values='2', operator='>')
        self.assertEqual(3, filtered_df.shape[0])
        self.assertEqual({3, 4, 5}, set(filtered_df['id']))

        # Test filtering a Pandas DataFrame using Transude with multiple values and a different operator
        filtered_df = txd.filter_df(data_frame=self.pd_df, columns='size', values=['small', 'medium'],
                                    operator='==', joiner='or', group_joiner='or')
        self.assertEqual(4, filtered_df.shape[0])
        self.assertEqual({'small', 'medium'}, set(filtered_df['size']))

    def test_filter_pandas_multiple_columns_and(self):
        filtered_df = txd.filter_df(data_frame=self.pd_df, columns=['color', 'size'], values=['red', 'small'],
                                    operator='==', joiner='and')
        self.assertEqual(1, filtered_df.shape[0])
        self.assertEqual({'red'}, set(filtered_df['color']))
        self.assertEqual({'small'}, set(filtered_df['size']))

    def test_filter_pandas_contains(self):
        filtered_df = txd.filter_df(data_frame=self.pd_df, columns='color', values='ue', operator='contains')
        self.assertEqual(2, filtered_df.shape[0])
        self.assertEqual({'blue'}, set(filtered_df['color']))

    def test_filter_pandas_startswith(self):
        filtered_df = txd.filter_df(data_frame=self.pd_df, columns='color', values='b', operator='startswith')
        self.assertEqual(2, filtered_df.shape[0])
        self.assertEqual({'blue'}, set(filtered_df['color']))

    def test_filter_pandas_endswith(self):
        filtered_df = txd.filter_df(data_frame=self.pd_df, columns='color', values='ue', operator='endswith')
        self.assertEqual(2, filtered_df.shape[0])
        self.assertEqual({'blue'}, set(filtered_df['color']))

    def test_filter_pandas_match(self):
        filtered_df = txd.filter_df(data_frame=self.pd_df, columns='color', values='^b.*', operator='match')
        self.assertEqual(2, filtered_df.shape[0])
        self.assertEqual({'blue'}, set(filtered_df['color']))

    def test_filter_pandas_invalid_operator(self):
        self.assertRaises(ValueError, txd.filter_df, data_frame=self.pd_df, columns='color', values='red', operator='!>')


class TestTransudeFunctions(unittest.TestCase):
    def setUp(self):
        self.data_frame = pd.DataFrame({
            'column_1': ['A', 'B', 'C', 'D', 'E'],
            'column_2': [1, 2, 3, 4, 5],
            'column_3': [10.1, 20.2, 30.3, 40.4, 50.5],
            'column_4': [True, False, True, False, True],
            'column_5': [datetime.now(), datetime.now(), datetime.now(), datetime.now(), datetime.now()]
        })

    def test_filter_df(self):
        # Test filtering on a single column
        filtered_df = txd.filter_df(self.data_frame, 'column_1', 'A', '==')
        self.assertEqual(1, filtered_df.shape[0])
        self.assertEqual('A',filtered_df.iloc[0]['column_1'])

        # Test filtering on multiple columns
        filtered_df = txd.filter_df(self.data_frame, ['column_1', 'column_2'], ['A', 1], '==')
        self.assertEqual(1, filtered_df.shape[0])
        self.assertEqual('A', filtered_df.iloc[0]['column_1'])
        self.assertEqual(1, filtered_df.iloc[0]['column_2'])

        # Test filtering with different operator
        filtered_df = txd.filter_df(self.data_frame, 'column_2', '3', '>')
        self.assertEqual(2, filtered_df.shape[0])
        self.assertEqual(4, filtered_df.iloc[0]['column_2'])
        self.assertEqual(5, filtered_df.iloc[1]['column_2'])

        # Test filtering with different joiner
        filtered_df = txd.filter_df(self.data_frame, ['column_1', 'column_2'], ['A', 1], '==', joiner='or')
        self.assertEqual(1, filtered_df.shape[0])


if __name__ == '__main__':
    unittest.main()
