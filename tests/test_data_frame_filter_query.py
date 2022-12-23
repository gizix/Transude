import unittest
from src.data_frame_filter import DataFrameFilterFactory
from src.data_frame_filter_query import DataFrameFilterQuery


class TestDataFrameFilterQuery(unittest.TestCase):
    def test_data_frame_filter_query(self):
        # Create an instance of DataFrameFilterQuery
        filter_query = DataFrameFilterQuery()

        # Use DataFrameFilterFactory to create a DataFrameFilter object
        filter1 = DataFrameFilterFactory.create_data_frame_filter(columns='COL_1',
                                                                  dtypes='float',
                                                                  operators='==',
                                                                  values=0.0,
                                                                  match_case=False,
                                                                  regex=False,
                                                                  searching=False,
                                                                  toggled=False,
                                                                  force_empty_value=False,
                                                                  joiner='AND')

        # Test the behavior of the add_filter method
        filter_query.add_filter(filter1)
        self.assertEqual([filter1], filter_query.filters)

        # Test the behavior of the add_unique_filter method
        filter_query.add_unique_filter(filter1)
        self.assertEqual([filter1], filter_query.filters)

        # Test the behavior of the rem_filter method
        self.assertTrue(filter_query.rem_filter(filter1))
        self.assertEqual([], filter_query.filters)

        # Test the behavior of the rem_filter_by_index method
        filter_query.add_filter(filter1)
        self.assertTrue(filter_query.rem_filter_by_index(0))
        self.assertEqual([], filter_query.filters)
        self.assertFalse(filter_query.rem_filter_by_index(0))

        # Test the behavior of the rem_non_global_non_toggled_filter_by_index method
        filter_query.add_filter(filter1)
        self.assertTrue(filter_query.rem_non_search_non_toggled_filter_by_index(0))
        self.assertEqual([], filter_query.filters)
        self.assertFalse(filter_query.rem_non_search_non_toggled_filter_by_index(0))

        # Test the behavior of the rem_non_global_filter_by_index method
        filter_query.add_filter(filter1)
        self.assertTrue(filter_query.rem_non_search_filter_by_index(0))
        self.assertEqual([], filter_query.filters)
        self.assertFalse(filter_query.rem_non_search_filter_by_index(0))

        # Test the behavior of the rem_search_filters method
        filter_query.add_filter(filter1)
        self.assertFalse(filter_query.remove_search_filters())
        self.assertEqual([filter1], filter_query.filters)  # The filter is not searching

        # Test the behavior of the clear_all method
        filter_query.add_filter(filter1)
        filter_query.clear_all()
        self.assertEqual([], filter_query.filters)

    def test_get_full_query(self):
        # Create an instance of DataFrameFilterQuery
        filter_query = DataFrameFilterQuery()

        # Use DataFrameFilterFactory to create a DataFrameFilter object
        filter1 = DataFrameFilterFactory.create_data_frame_filter(columns='COL_1',
                                                                  dtypes='float',
                                                                  operators='==',
                                                                  values=0.0,
                                                                  match_case=False,
                                                                  regex=False,
                                                                  searching=False,
                                                                  toggled=False,
                                                                  force_empty_value=False,
                                                                  joiner='AND')

        # Add the DataFrameFilter object to the DataFrameFilterQuery object
        filter_query.add_filter(filter1)

        # Test the behavior of the get_full_query method
        expected_query = "COL_1 == 0.0"
        self.assertEqual(expected_query, filter_query.get_full_query())

