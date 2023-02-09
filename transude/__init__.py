import pandas as pd
from datetime import datetime
from typing import Union, List
from .data_frame_filter import DataFrameFilter
from .data_frame_filter_factory import DataFrameFilterFactory
from .data_frame_filter_manager import DataFrameFilterManager

ValueMultiTyping = Union[Union[str, List[str]], Union[str, List[int]], Union[str, List[float]],
                         Union[str, List[bool]], Union[str, List[datetime.date]]]


def filter_df(data_frame: pd.DataFrame,
              columns: Union[str, List[str]],
              values: ValueMultiTyping,
              operator: str,
              joiner: str = 'and',
              match_case: bool = False,
              regex: bool = False,
              omit_on_clear: bool = False,
              common_name: str = None,
              group_joiner: str = None) -> pd.DataFrame:
    """
    Filters a data frame based on a list of columns and values.

    :param data_frame:      The data frame to filter.
    :param columns:         The columns to filter on.
    :param values:          The values to filter on.
    :param operator:        The operator to use.
    :param joiner:          The joiner to use.
    :param match_case:      Option to match case.
    :param regex:           Option to use regex.
    :param omit_on_clear:   Option to omit on clear.
    :param common_name:     Specified common description.
    :param group_joiner:    The group joiner to use.
    :return:                The filtered data frame.
    """
    if isinstance(data_frame, pd.DataFrame):
        df_factory = DataFrameFilterFactory(columns=columns,
                                            values=values,
                                            operator=operator,
                                            joiner=joiner,
                                            data_frame=data_frame,
                                            match_case=match_case,
                                            regex=regex,
                                            omit_on_clear=omit_on_clear,
                                            common_name=common_name,
                                            group_joiner=group_joiner)
        df_filters = df_factory.create_filters()
        query_builder = DataFrameFilterManager(df_filters)
        query = query_builder.build_query()
        return data_frame if not query else data_frame.query(query)
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
        return data_frame if not query else data_frame.query(query)
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
        return data_frame if not query else data_frame.query(query)
    else:
        raise TypeError(f"Unrecognized data frame type: {type(data_frame)}")


def build_df_filters(columns: Union[str, List[str]],
                     values: ValueMultiTyping,
                     operator: str,
                     joiner: str = 'and',
                     data_frame: pd.DataFrame = None,
                     match_case: bool = False,
                     regex: bool = False,
                     omit_on_clear: bool = False,
                     common_name: str = None,
                     group_joiner: str = None) -> List[DataFrameFilter]:
    """
    Builds a list of DataFrameFilter objects.

    :param columns:         The columns to filter on.
    :param values:          The values to filter on.
    :param operator:        The operator to use.
    :param joiner:          The joiner to use.
    :param data_frame:      The data frame to filter.
    :param match_case:      Option to match case.
    :param regex:           Option to use regex.
    :param omit_on_clear:   Option to omit on clear.
    :param common_name:     Specified common description.
    :param group_joiner:    The group joiner to use.
    :return:                List of DataFrameFilters.
    """
    df_filter_factory = DataFrameFilterFactory(columns=columns,
                                               values=values,
                                               operator=operator,
                                               joiner=joiner,
                                               data_frame=data_frame,
                                               match_case=match_case,
                                               regex=regex,
                                               omit_on_clear=omit_on_clear,
                                               common_name=common_name,
                                               group_joiner=group_joiner)
    return df_filter_factory.create_filters()


def build_query_from_df_filters(df_filters: List[DataFrameFilter]) -> str:
    """
    Builds a DataFrame query from a list of DataFrameFilter objects.

    :param df_filters:  A list of DataFrameFilter objects.
    :return:  The DataFrame query.
    """
    query_builder = DataFrameFilterManager(df_filters)
    return query_builder.build_query()
