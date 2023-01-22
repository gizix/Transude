import unittest
import pandas as pd
import transude as txd


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
                                    operator='==', joiner='or')
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
        self.assertRaises(ValueError, txd.filter_df, data_frame=self.pd_df, columns='color', values='red', operator='!=')


if __name__ == '__main__':
    unittest.main()
