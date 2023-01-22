import pandas
import pandas as pd
from datetime import datetime
from typing import Union, List
from .data_frame_filter import DataFrameFilter
from .data_frame_filter_factory import DataFrameFilterFactory
from .data_frame_filter_manager import DataFrameFilterManager


def filter_df(data_frame: pd.DataFrame,
              columns: Union[str, List[str]],
              values: Union[Union[str, List[str]], Union[str, List[int]], Union[str, List[float]],
                            Union[str, List[bool]], Union[str, List[datetime.date]]],
              operator: str,
              joiner: str = 'and') -> pd.DataFrame:
    """
    Filters a data frame based on a list of columns and values.

    :param data_frame:  The data frame to filter.
    :param columns:     The columns to filter on.
    :param values:      The values to filter on.
    :param operator:    The operator to use.
    :param joiner:      The joiner to use.
    :return:            The filtered data frame.
    """
    if isinstance(data_frame, pd.DataFrame):
        df_factory = DataFrameFilterFactory(columns=columns, values=values, operator=operator, joiner=joiner, data_frame=data_frame)
        df_filters = df_factory.create_filters()
        query_builder = DataFrameFilterManager(df_filters)
        query = query_builder.build_query()
        return data_frame.query(query)
    else:
        raise TypeError(f"Unrecognized data frame type: {type(data_frame)}")


def filter_df_from_df_filters(data_frame: pd.DataFrame,
                              df_filters: List[DataFrameFilter]) -> pd.DataFrame:
    """
    Filters a data frame based on a list of DataFrameFilter objects.

    :param data_frame:  The data frame to filter.
    :param df_filters:  A list of DataFrameFilter objects.
    :return:  The filtered data frame.
    """
    if isinstance(data_frame, pd.DataFrame):
        query_builder = DataFrameFilterManager(df_filters)
        query = query_builder.build_query()
        return data_frame.query(query)
    else:
        raise TypeError(f"Unrecognized data frame type: {type(data_frame)}")


def filter_df_via_manager(data_frame: pd.DataFrame, df_filter_manager: DataFrameFilterManager) -> pd.DataFrame:
    """
    Filters a data frame based on a DataFrameFilterManager object.

    :param data_frame:  The data frame to filter.
    :param df_filter_manager:  A DataFrameFilterManager object.
    :return:  The filtered data frame.
    """
    if isinstance(data_frame, pd.DataFrame):
        query = df_filter_manager.build_query()
        return data_frame.query(query)
    else:
        raise TypeError(f"Unrecognized data frame type: {type(data_frame)}")


def build_df_filters(columns: Union[str, List[str]],
                     values: Union[Union[str, List[str]], Union[str, List[int]], Union[str, List[float]],
                                   Union[str, List[bool]], Union[str, List[datetime.date]]],
                     operator: str,
                     joiner: str = 'and',
                     data_frame: pd.DataFrame = None) -> List[DataFrameFilter]:
    """
    Builds a list of DataFrameFilter objects.

    :param columns:     The columns to filter on.
    :param values:      The values to filter on.
    :param operator:    The operator to use.
    :param joiner:      The joiner to use.
    :param data_frame:  The data frame to filter.
    :return:  list of filters
    """
    df_filter_factory = DataFrameFilterFactory(columns=columns, values=values, operator=operator, joiner=joiner, data_frame=data_frame)
    return df_filter_factory.create_filters()


def build_query_from_df_filters(df_filters: List[DataFrameFilter]) -> str:
    """
    Builds a DataFrame query from a list of DataFrameFilter objects.

    :param df_filters:  A list of DataFrameFilter objects.
    :return:  The DataFrame query.
    """
    query_builder = DataFrameFilterManager(df_filters)
    return query_builder.build_query()
