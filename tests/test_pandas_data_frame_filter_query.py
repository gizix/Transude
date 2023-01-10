import itertools
import pandas as pd
import unittest
import src.transude.transude as txd
from datetime import datetime
from src.transude.pandas.data_frame_filter import DataFrameFilter
from src.transude.pandas.data_frame_query_builder import DataFrameQueryBuilder
from src.transude.pandas.data_frame_filter_factory import DataFrameFilterFactory


class TestPandasDataFrameFilter(unittest.TestCase):
    def test_get_query(self):
        df_filter = DataFrameFilter(column='col1', value='val1', operator='==')
        self.assertEqual("col1 == 'val1'", df_filter.get_query())

        df_filter = DataFrameFilter(column='col1', value='val1', operator='contains')
        self.assertEqual("col1.str.contains('val1')", df_filter.get_query())

        df_filter = DataFrameFilter(column='col1', value='val1', operator='>')
        self.assertEqual("col1 > 'val1'", df_filter.get_query())

        df_filter = DataFrameFilter(column='col1', value='val1', operator='<')
        self.assertEqual("col1 < 'val1'", df_filter.get_query())

        df_filter = DataFrameFilter(column='col1', value='val1', operator='>=')
        self.assertEqual("col1 >= 'val1'", df_filter.get_query())

        df_filter = DataFrameFilter(column='col1', value='val1', operator='<=')
        self.assertEqual("col1 <= 'val1'", df_filter.get_query())

        df_filter = DataFrameFilter(column='col1', value='val1', operator='contains', match_case=True, regex=True)
        self.assertEqual("col1.str.contains('val1', case=True, regex=True)", df_filter.get_query())

        df_filter = DataFrameFilter(column='col1', value='val1', operator='contains', match_case=True)
        self.assertEqual("col1.str.contains('val1', case=True, regex=False)", df_filter.get_query())

        df_filter = DataFrameFilter(column='col1', value='val1', operator='contains', regex=True)
        self.assertEqual("col1.str.contains('val1', case=False, regex=True)", df_filter.get_query())

        df_filter = DataFrameFilter(column='col1', value='val1', operator='==', match_case=True, regex=True)
        self.assertEqual("col1 == 'val1'", df_filter.get_query())

    def test_repr(self):
        df_filter = DataFrameFilter(column='col1', value='val1', operator='==', joiner='and')
        self.assertEqual("DataFrameFilter(column='col1', value='val1', operator='==', joiner='and', filter_id=None, "
                         "match_case=False, regex=False)",
                         repr(df_filter))

    def test_str(self):
        df_filter = DataFrameFilter(column='col1', value='val1', operator='==')
        self.assertEqual("col1 == 'val1'", str(df_filter))

    def test_is_valid_operator(self):
        self.assertTrue(DataFrameFilter.is_valid_operator('=='))
        self.assertTrue(DataFrameFilter.is_valid_operator('>'))
        self.assertTrue(DataFrameFilter.is_valid_operator('<'))
        self.assertTrue(DataFrameFilter.is_valid_operator('>='))
        self.assertTrue(DataFrameFilter.is_valid_operator('<='))
        self.assertTrue(DataFrameFilter.is_valid_operator('contains'))
        self.assertTrue(DataFrameFilter.is_valid_operator('startswith'))
        self.assertTrue(DataFrameFilter.is_valid_operator('endswith'))
        self.assertTrue(DataFrameFilter.is_valid_operator('match'))
        self.assertFalse(DataFrameFilter.is_valid_operator('invalid'))

    def test_data_frame_filter_with_invalid_operator(self):
        with self.assertRaises(ValueError):
            df_filter = DataFrameFilter(column='col1', value='val1', operator='invalid')


class TestPandasDataFrameQueryBuilder(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({'col1': ['val1', 'val2', 'val3'], 'col2': ['val4', 'val5', 'val6']})
        self.df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==')
        self.df_filter2 = DataFrameFilter(column='col2', value='val4', operator='==', joiner='and')
        self.df_query_builder = DataFrameQueryBuilder()

    def test_build_query(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', joiner='and')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        self.assertEqual("(col1 == 'val1') and (col2 == 'val2')", df_query_builder.build_query())

    def test_build_query_and_query(self):
        self.df_query_builder.add_filter(self.df_filter1)
        self.df_query_builder.add_filter(self.df_filter2)
        query = self.df_query_builder.build_query()
        result = self.df.query(query)
        self.assertEqual(1, len(result))
        self.assertEqual('val1', result.iloc[0]['col1'])
        self.assertEqual('val4', result.iloc[0]['col2'])

    def test_build_query_with_contains_operator(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='contains')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='contains', joiner='and')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        self.assertEqual("(col1.str.contains('val1')) and (col2.str.contains('val2'))", df_query_builder.build_query())

    def test_build_query_with_startswith_operator(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='startswith')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='startswith', joiner='or')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        self.assertEqual("(col1.str.startswith('val1')) or (col2.str.startswith('val2'))", df_query_builder.build_query())

    def test_build_query_with_endswith_operator(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='endswith')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='endswith', joiner='and')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        self.assertEqual("(col1.str.endswith('val1')) and (col2.str.endswith('val2'))", df_query_builder.build_query())

    def test_build_query_with_match_operator(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='match')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='match', joiner='or')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        self.assertEqual("(col1.str.match('val1')) or (col2.str.match('val2'))", df_query_builder.build_query())

    def test_build_query_with_non_str_operators(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', joiner='and')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        self.assertEqual("(col1 == 'val1') and (col2 == 'val2')", df_query_builder.build_query())

        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='>')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='>', joiner='or')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        self.assertEqual("(col1 > 'val1') or (col2 > 'val2')", df_query_builder.build_query())

        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='<')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='<', joiner='and')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        self.assertEqual("(col1 < 'val1') and (col2 < 'val2')", df_query_builder.build_query())

        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='>=')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='>=', joiner='or')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        self.assertEqual("(col1 >= 'val1') or (col2 >= 'val2')", df_query_builder.build_query())

        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='<=')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='<=', joiner='and')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        self.assertEqual("(col1 <= 'val1') and (col2 <= 'val2')", df_query_builder.build_query())

    def test_add_and_remove_filters(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', joiner='and')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        self.assertEqual([df_filter1, df_filter2], df_query_builder.data_frame_filters)

        df_query_builder.remove_filter(df_filter1)
        self.assertEqual([df_filter2], df_query_builder.data_frame_filters)

        df_query_builder.remove_filter_by_index(0)
        self.assertEqual([], df_query_builder.data_frame_filters)

    def test_remove_filter_by_index(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', joiner='and')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)

        df_query_builder.remove_filter_by_index(0)
        self.assertEqual([df_filter2], df_query_builder.data_frame_filters)

        df_query_builder.remove_filter_by_index(0)
        self.assertEqual([], df_query_builder.data_frame_filters)

        with self.assertRaises(IndexError):
            df_query_builder.remove_filter_by_index(0)

    def test_clear_filters(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', joiner='and')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)

        df_query_builder.clear_filters()
        self.assertEqual([], df_query_builder.data_frame_filters)

    def test_disable_filters(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==', in_use=True)
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', joiner='and', in_use=True)
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)

        df_query_builder.disable_filters()
        self.assertFalse(df_filter1.in_use)
        self.assertFalse(df_filter2.in_use)

    def test_build_query_with_none_joiner(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==', in_use=True)
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', joiner=None, in_use=True)
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)

        self.assertEqual("(col1 == 'val1') and (col2 == 'val2')", df_query_builder.build_query())

    def test_build_query_with_all_filters_disabled(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==', in_use=False)
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', joiner='or', in_use=False)
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)

        self.assertEqual("", df_query_builder.build_query())

    def test_build_query_with_empty_filters(self):
        df_query_builder = DataFrameQueryBuilder()

        self.assertEqual("", df_query_builder.build_query())

    def test_build_query_with_single_filter_none_joiner(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==', joiner=None, in_use=True)
        df_query_builder.add_filter(df_filter1)

        self.assertEqual("(col1 == 'val1')", df_query_builder.build_query())

    def test_remove_filters_by_id(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==', filter_id=1)
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', filter_id=2)
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)

        df_query_builder.remove_filters_by_id(1)
        self.assertEqual(1, len(df_query_builder.data_frame_filters))
        self.assertEqual("col2 == 'val2'", str(df_query_builder.data_frame_filters[0]))

    def test_remove_filters_by_id_with_valid_id(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==', filter_id=1)
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', filter_id=2)
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        df_query_builder.remove_filters_by_id(filter_id=1)

        self.assertEqual(1, len(df_query_builder.data_frame_filters))
        self.assertEqual("col2 == 'val2'", str(df_query_builder.data_frame_filters[0]))

    def test_remove_filters_by_id_non_existent(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==', in_use=True, filter_id=1)
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', joiner=None, in_use=True, filter_id=2)
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)

        with self.assertRaises(ValueError):
            df_query_builder.remove_filters_by_id(3)

    def test_add_filter_with_invalid_operator(self):
        df_query_builder = DataFrameQueryBuilder()
        with self.assertRaises(ValueError):
            df_query_builder.add_filter(DataFrameFilter(column='col1', value='val1', operator='invalid'))

    def test_build_query_with_in_use_filters(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==', in_use=True)
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', in_use=False)
        df_filter3 = DataFrameFilter(column='col3', value='val3', operator='==', in_use=True)
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        df_query_builder.add_filter(df_filter3)

        self.assertEqual("(col1 == 'val1') and (col3 == 'val3')", df_query_builder.build_query())

    def test_build_query_with_different_joiners(self):
        df_query_builder = DataFrameQueryBuilder()

        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='==', in_use=True, joiner='or')
        df_filter2 = DataFrameFilter(column='col2', value='val2', operator='==', in_use=True, joiner='and')
        df_filter3 = DataFrameFilter(column='col3', value='val3', operator='==', in_use=True, joiner='or')
        df_query_builder.add_filter(df_filter1)
        df_query_builder.add_filter(df_filter2)
        df_query_builder.add_filter(df_filter3)

        self.assertEqual("(col1 == 'val1') and (col2 == 'val2') or (col3 == 'val3')", df_query_builder.build_query())

    def test_build_query_with_contains_operator_and_match_case(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='contains', match_case=True)
        df_query_builder.add_filter(df_filter1)
        self.assertEqual("(col1.str.contains('val1', case=True, regex=False))", df_query_builder.build_query())

    def test_build_query_with_contains_operator_and_regex(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='contains', regex=True)
        df_query_builder.add_filter(df_filter1)
        self.assertEqual("(col1.str.contains('val1', case=False, regex=True))", df_query_builder.build_query())

    def test_build_query_with_contains_operator_and_match_case_and_regex(self):
        df_query_builder = DataFrameQueryBuilder()
        df_filter1 = DataFrameFilter(column='col1', value='val1', operator='contains', match_case=True, regex=True)
        df_query_builder.add_filter(df_filter1)
        self.assertEqual("(col1.str.contains('val1', case=True, regex=True))", df_query_builder.build_query())

    def test_disable_filters_by_id(self):
        # Create a DataFrameFilter with filter_id=1
        df_filter1 = DataFrameFilter(column='col1', value='value1', operator='==', filter_id=1)
        # Create a DataFrameFilter with filter_id=2
        df_filter2 = DataFrameFilter(column='col2', value='value2', operator='==', filter_id=2)
        # Create a DataFrameQueryBuilder with the two filters
        query_builder = DataFrameQueryBuilder([df_filter1, df_filter2])
        # Ensure both filters are in use
        self.assertTrue(df_filter1.in_use)
        self.assertTrue(df_filter2.in_use)
        # Disable filters with filter_id=1
        query_builder.disable_filters_by_id(1)
        # Ensure only the filter with filter_id=1 is disabled
        self.assertFalse(df_filter1.in_use)
        self.assertTrue(df_filter2.in_use)
        # Try to disable filters with a filter_id that does not exist
        query_builder.disable_filters_by_id(3)
        self.assertFalse(df_filter1.in_use)
        self.assertTrue(df_filter2.in_use)

    def test_enable_filters_by_id(self):
        # Create some DataFrameFilters and add them to a DataFrameQueryBuilder
        df_filter1 = DataFrameFilter(column="column1", value="value1", operator="==", filter_id=1)
        df_filter2 = DataFrameFilter(column="column2", value="value2", operator="==", filter_id=2)
        query_builder = DataFrameQueryBuilder([df_filter1, df_filter2])
        # Disable one of the filters and verify that it was correctly disabled
        df_filter1.in_use = False
        self.assertFalse(df_filter1.in_use)
        self.assertTrue(df_filter2.in_use)
        # Enable the disabled filter using its filter ID and verify that it was correctly enabled
        query_builder.enable_filters_by_id(1)
        self.assertTrue(df_filter1.in_use)
        self.assertTrue(df_filter2.in_use)


class TestPandasDataFrameFilterFactory(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            'col1': ['val1', 'val2', 'val3'],
            'col2': ['val4', 'val5', 'val6'],
            'col3': [1, 2, 3],
            'col4': [1.1, 2.2, 3.3],
            'col5': [True, False, True],
            'col6': [datetime(2022, 1, 1), datetime(2022, 2, 2), datetime(2022, 3, 3)]
        })
        self.reset_counter()  # reset the counter before each test case

    @staticmethod
    def reset_counter():
        DataFrameFilterFactory.next_filter_id = itertools.count()  # reset the counter to its initial value

    def tearDown(self):
        self.reset_counter()  # reset the counter after each test case

    def test_create_filters_for_single_column_and_value(self):
        factory = DataFrameFilterFactory(columns='col1', values='val1', operator='==')
        filters = factory.create_filters()
        self.assertEqual(1, len(filters))
        self.assertEqual('col1', filters[0].column)
        self.assertEqual('val1', filters[0].value)
        self.assertEqual('==', filters[0].operator)
        self.assertEqual('and', filters[0].joiner)
        self.assertEqual(1, filters[0].filter_id)

    def test_create_filters_for_single_column_and_multiple_values(self):
        factory = DataFrameFilterFactory(columns='col1', values=['val1', 'val2'], operator='==')
        filters = factory.create_filters()
        self.assertEqual(2, len(filters))
        self.assertEqual('col1', filters[0].column)
        self.assertEqual('val1', filters[0].value)
        self.assertEqual('==', filters[0].operator)
        self.assertEqual('and', filters[0].joiner)
        self.assertEqual(1, filters[0].filter_id)
        self.assertEqual('col1', filters[1].column)
        self.assertEqual('val2', filters[1].value)
        self.assertEqual('==', filters[1].operator)
        self.assertEqual('and', filters[1].joiner)
        self.assertEqual(1, filters[1].filter_id)

    def test_create_filters_for_multiple_columns_and_values(self):
        factory = DataFrameFilterFactory(columns=['col1', 'col2'], values=['val1', 'val4'], operator='==')
        filters = factory.create_filters()
        self.assertEqual(2, len(filters))
        self.assertEqual('col1', filters[0].column)
        self.assertEqual('val1', filters[0].value)
        self.assertEqual('==', filters[0].operator)
        self.assertEqual('and', filters[0].joiner)
        self.assertEqual(1, filters[0].filter_id)
        self.assertEqual('col2', filters[1].column)
        self.assertEqual('val4', filters[1].value)
        self.assertEqual('==', filters[1].operator)
        self.assertEqual('and', filters[1].joiner)
        self.assertEqual(1, filters[1].filter_id)

    def test_create_filters_with_custom_joiner(self):
        factory = DataFrameFilterFactory(columns='col1', values=['val1', 'val2'], operator='==', joiner='or')
        filters = factory.create_filters()
        self.assertEqual(2, len(filters))
        self.assertEqual('col1', filters[0].column)
        self.assertEqual('val1', filters[0].value)
        self.assertEqual('==', filters[0].operator)
        self.assertEqual('or', filters[0].joiner)
        self.assertEqual(1, filters[0].filter_id)
        self.assertEqual('col1', filters[1].column)
        self.assertEqual('val2', filters[1].value)
        self.assertEqual('==', filters[1].operator)
        self.assertEqual('or', filters[1].joiner)
        self.assertEqual(1, filters[1].filter_id)

    def test_create_filters_with_custom_id(self):
        factory = DataFrameFilterFactory(columns='col1', values=['val1', 'val2'], operator='==', filter_id=10)
        filters = factory.create_filters()
        self.assertEqual(2, len(filters))
        self.assertEqual('col1', filters[0].column)
        self.assertEqual('val1', filters[0].value)
        self.assertEqual('==', filters[0].operator)
        self.assertEqual('and', filters[0].joiner)
        self.assertEqual(10, filters[0].filter_id)
        self.assertEqual('col1', filters[1].column)
        self.assertEqual('val2', filters[1].value)
        self.assertEqual('==', filters[1].operator)
        self.assertEqual('and', filters[1].joiner)
        self.assertEqual(10, filters[1].filter_id)

    def test_create_filters_for_single_column_and_multiple_integer_values(self):
        factory = DataFrameFilterFactory(columns='col1', values=[1, 2, 3], operator='==')
        filters = factory.create_filters()
        self.assertEqual(3, len(filters))
        self.assertEqual('col1', filters[0].column)
        self.assertEqual('1', filters[0].value)
        self.assertEqual('==', filters[0].operator)
        self.assertEqual('and', filters[0].joiner)
        self.assertEqual(1, filters[0].filter_id)
        self.assertEqual('col1', filters[1].column)
        self.assertEqual('2', filters[1].value)
        self.assertEqual('==', filters[1].operator)
        self.assertEqual('and', filters[1].joiner)
        self.assertEqual(1, filters[1].filter_id)
        self.assertEqual('col1', filters[2].column)
        self.assertEqual('3', filters[2].value)
        self.assertEqual('==', filters[2].operator)
        self.assertEqual('and', filters[2].joiner)
        self.assertEqual(1, filters[2].filter_id)

    def test_factory_creation_with_invalid_operator(self):
        with self.assertRaises(ValueError):
            factory = DataFrameFilterFactory(columns='col1', values='val1', operator='invalid')

    def test_construct_query_builder_with_filters(self):
        factory = DataFrameFilterFactory(columns='col1', values=['val1', 'val2'], operator='==', joiner='or')
        df_filters = factory.create_filters()
        query_builder = DataFrameQueryBuilder(df_filters)
        self.assertEqual(df_filters, query_builder.data_frame_filters)
        query = query_builder.build_query()
        self.assertEqual("(col1 == 'val1') or (col1 == 'val2')", query)
        filtered_df = txd.filter_df(self.df, columns='col1', values=['val1', 'val2'], operator='==', joiner='or')
        pd.testing.assert_frame_equal(self.df.query(query), filtered_df)
