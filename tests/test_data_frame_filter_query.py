import datetime
import unittest
import pandas as pd
from ChatGPT.data_frame_filter_no_docstrings import DataFrameFilterFactory
from ChatGPT.data_frame_filter_query_no_docstrings import DataFrameFilterQuery


class TestDataFrameFilterQuery(unittest.TestCase):
    def setUp(self):
        # Create a list of tuples representing the rows of the DataFrame
        rows = [
            (1, datetime.date(2022, 1, 1), "A", True, 1.0),
            (2, datetime.date(2022, 1, 2), "B", False, 2.0),
            (3, datetime.date(2022, 1, 3), "C", True, 3.0),
            (4, datetime.date(2022, 1, 4), "D", False, 4.0),
            (5, datetime.date(2022, 1, 5), "E", True, 5.0)
        ]

        # Create a list of column names
        columns = ["COL_1", "COL_2", "COL_3", "COL_4", "COL_5"]

        # Create the DataFrame
        self.test_df = pd.DataFrame(rows, columns=columns)

        # Use DataFrameFilterFactory to create DataFrameFilter objects
        self.df_filter1 = DataFrameFilterFactory.create_data_frame_filter(columns='COL_1',
                                                                          dtypes='int',
                                                                          operator='==',
                                                                          values=3,
                                                                          match_case=False,
                                                                          regex=False,
                                                                          searching=False,
                                                                          toggled=False,
                                                                          force_empty_value=False)

        self.df_filter2 = DataFrameFilterFactory.create_data_frame_filter(columns='COL_2',
                                                                          dtypes='datetime',
                                                                          operator='==',
                                                                          values=datetime.date(2022, 1, 1),
                                                                          match_case=False,
                                                                          regex=False,
                                                                          searching=False,
                                                                          toggled=False,
                                                                          force_empty_value=False)

        self.df_filter3 = DataFrameFilterFactory.create_data_frame_filter(columns='COL_3',
                                                                          dtypes='str',
                                                                          operator='==',
                                                                          values='D',
                                                                          match_case=True,
                                                                          regex=False,
                                                                          searching=False,
                                                                          toggled=False,
                                                                          force_empty_value=False)

        self.df_filter4 = DataFrameFilterFactory.create_data_frame_filter(columns='COL_4',
                                                                          dtypes='bool',
                                                                          operator='!=',
                                                                          values=False,
                                                                          match_case=True,
                                                                          regex=False,
                                                                          searching=False,
                                                                          toggled=False,
                                                                          force_empty_value=False)

        self.df_filter5 = DataFrameFilterFactory.create_data_frame_filter(columns='COL_5',
                                                                          dtypes='float',
                                                                          operator='<=',
                                                                          values=4.0,
                                                                          match_case=False,
                                                                          regex=False,
                                                                          searching=False,
                                                                          toggled=False,
                                                                          force_empty_value=False)

        self.df_filter6 = DataFrameFilterFactory.create_data_frame_filter(columns=['COL_1', 'COL_2', 'COL_3'],
                                                                          dtypes=['int', 'datetime', 'string'],
                                                                          operator='==',
                                                                          values=[3, 'C', 4.0],
                                                                          match_case=True,
                                                                          regex=False,
                                                                          searching=True,
                                                                          toggled=False,
                                                                          force_empty_value=False,
                                                                          joiner='OR')

        self.df_filter7 = DataFrameFilterFactory.create_data_frame_filter(columns='COL_1',
                                                                          dtypes='int',
                                                                          operator='==',
                                                                          values=1,
                                                                          match_case=False,
                                                                          regex=False,
                                                                          searching=False,
                                                                          toggled=False,
                                                                          force_empty_value=False)

        self.df_filter8 = DataFrameFilterFactory.create_data_frame_filter(columns='COL_2',
                                                                          dtypes='datetime',
                                                                          operator='==',
                                                                          values=datetime.date(2022, 1, 1),
                                                                          match_case=False,
                                                                          regex=False,
                                                                          searching=False,
                                                                          toggled=False,
                                                                          force_empty_value=False)

        self.df_filter9 = DataFrameFilterFactory.create_data_frame_filter(columns='COL_3',
                                                                          dtypes='str',
                                                                          operator='==',
                                                                          values='A',
                                                                          match_case=True,
                                                                          regex=False,
                                                                          searching=False,
                                                                          toggled=False,
                                                                          force_empty_value=False)

        self.df_filters = [self.df_filter1, self.df_filter2, self.df_filter3, self.df_filter4, self.df_filter5,
                           self.df_filter6, self.df_filter7, self.df_filter8, self.df_filter9]

    def test_data_frame_filter_query(self):
        # Create an instance of DataFrameFilterQuery
        filter_query = DataFrameFilterQuery()

        # Test the behavior of the add_filter method
        filter_query.add_filter(self.df_filter1)
        self.assertListEqual([self.df_filter1], filter_query.data_frame_filters)

        # Test the behavior of the add_unique_filter method
        filter_query.add_unique_filter(self.df_filter1)
        self.assertListEqual([self.df_filter1], filter_query.data_frame_filters)

        # Test the behavior of the rem_filter method
        self.assertIs(filter_query.remove_filter(self.df_filter1), True)
        self.assertListEqual([], filter_query.data_frame_filters)

        # Test the behavior of the rem_filter_by_index method
        filter_query.add_filter(self.df_filter1)
        self.assertIs(filter_query.remove_filter_by_index(0), True)
        self.assertListEqual([], filter_query.data_frame_filters)
        self.assertIsNot(filter_query.remove_filter_by_index(0), True)

        # Test the behavior of the rem_non_global_non_toggled_filter_by_index method
        filter_query.add_filter(self.df_filter1)
        self.assertIs(filter_query.remove_non_search_non_toggled_filter_by_index(0), True)
        self.assertListEqual([], filter_query.data_frame_filters)
        self.assertIsNot(filter_query.remove_non_search_non_toggled_filter_by_index(0), True)

        # Test the behavior of the rem_non_global_filter_by_index method
        filter_query.add_filter(self.df_filter1)
        self.assertIs(filter_query.remove_non_search_filter_by_index(0), True)
        self.assertListEqual([], filter_query.data_frame_filters)
        self.assertIsNot(filter_query.remove_non_search_filter_by_index(0), True)

        # Test the behavior of the rem_search_filters method
        filter_query.add_filter(self.df_filter1)
        self.assertIsNot(filter_query.remove_search_filters(), True)
        self.assertListEqual([self.df_filter1], filter_query.data_frame_filters)  # The filter is not searching

        # Test the behavior of the clear_all method
        filter_query.add_filter(self.df_filter1)
        filter_query.clear_all()
        self.assertListEqual([], filter_query.data_frame_filters)

    def test_get_full_query(self):
        # Create an instance of DataFrameFilterQuery
        filter_query = DataFrameFilterQuery()

        # Add the DataFrameFilter object to the DataFrameFilterQuery object
        filter_query.add_filter(self.df_filter1)

        # Test the behavior of the get_full_query method
        expected_query = "(COL_1 == 3)"
        self.assertEqual(expected_query, filter_query.get_full_query())

    def test_multiple_filters(self):
        # Create a DataFrameFilterQuery object with multiple DataFrameFilter objects
        filter_query = DataFrameFilterQuery([self.df_filter1, self.df_filter2, self.df_filter3])

        # Verify that the resulting query is correct
        expected_query = "(COL_1 == 3) & (COL_2 == '2022-01-01') & (COL_3 == 'D')"
        self.assertEqual(expected_query, filter_query.get_full_query())

    def test_filter(self):
        # Create a DataFrameFilterQuery object with the DataFrameFilter objects
        filter_query = DataFrameFilterQuery([self.df_filter1, self.df_filter2])
        print(filter_query.get_full_query())

        # Use the filter_query to filter the DataFrame
        filtered_df = self.test_df.query(filter_query.get_full_query())

        # Assert that the resulting DataFrame has the correct number of rows
        self.assertEqual(0, len(filtered_df))  # No joiner produces '&' by default resulting in 0 rows.

        # Assert that the resulting DataFrame has the correct column values
        self.assertEqual([], filtered_df['COL_1'].tolist())
        self.assertEqual([], filtered_df['COL_4'].tolist())

    def test_query(self):
        # Create a DataFrameFilterQuery object and get the query string
        df_filter_query = DataFrameFilterQuery([self.df_filter7, self.df_filter8, self.df_filter9], "or")
        query_str = df_filter_query.get_full_query()

        # Use the query string to filter the test DataFrame
        result = self.test_df.query(query_str)

        # Create an expected DataFrame
        expected_rows = [
            (1, datetime.date(2022, 1, 1), "A", True, 1.0)
        ]
        expected = pd.DataFrame(expected_rows, columns=self.test_df.columns)

        # Assert that the resulting DataFrame is as expected
        self.assertTrue(result.equals(expected))

