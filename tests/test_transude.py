import unittest
import pandas as pd
import polars as pl
import src.transude as txd


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

        self.pl_df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "color": ["blue", "red", "green", "red", "blue"],
                "size": ["small", "medium", "large", "small", "medium"],
            }
        )

    def test_filter_pandas(self):
        # Test filtering a Pandas DataFrame using Transude
        filtered_df = txd.filter_df(data_frame=self.pd_df, columns='color', values='red', operator='==')
        self.assertEqual(filtered_df.shape[0], 2)
        self.assertEqual(set(filtered_df['color']), {'red'})

        # Test filtering a Pandas DataFrame using Transude with multiple values and a different operator
        filtered_df = txd.filter_df(data_frame=self.pd_df, columns='size', values=['small', 'medium'], operator='==')
        self.assertEqual(filtered_df.shape[0], 2)
        self.assertEqual(set(filtered_df['size']), {'small', 'medium'})

    def test_filter_polars(self):
        # Test filtering a Polars DataFrame using Transude
        filtered_df = txd.filter_df(data_frame=self.pl_df, columns='color', values=['red'], operator='==')
        self.assertEqual(filtered_df.shape[0], 2)
        self.assertEqual(set(filtered_df['color']), {'red'})

        # Test filtering a Polars DataFrame using Transude with multiple values and a different operator
        filtered_df = txd.filter_df(data_frame=self.pl_df, columns='size', values=['small', 'medium'], operator='in')
        self.assertEqual(filtered_df.shape[0], 4)
        self.assertEqual(set(filtered_df['size']), {'small', 'medium'})


if __name__ == '__main__':
    unittest.main()
