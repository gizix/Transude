import pandas
import pandas as pd
import polars
import polars as pl
from datetime import datetime
from typing import Union, List
from src.transude.pandas.data_frame_filter_factory import DataFrameFilterFactory
from src.transude.pandas.data_frame_query_builder import DataFrameQueryBuilder


def filter_df(data_frame: Union[pandas.DataFrame, polars.DataFrame],
              columns: Union[str, List[str]],
              values: Union[Union[str, List[str]], Union[str, List[int]], Union[str, List[float]],
                            Union[str, List[bool]], Union[str, List[datetime.date]]],
              operator: str,
              joiner: str = 'and'):

    if isinstance(data_frame, pd.DataFrame):
        df_factory = DataFrameFilterFactory(columns=columns, values=values, operator=operator, joiner=joiner)
        df_filters = df_factory.create_filters()
        query_builder = DataFrameQueryBuilder(df_filters)
        query = query_builder.build_query()
        return data_frame.query(query)
    elif isinstance(data_frame, pl.DataFrame):
        filter_expression = build_filter_expression(columns, values, operator, joiner)
        return data_frame.filter(filter_expression)
    else:
        raise ValueError(f"Unrecognized data frame type: {type(data_frame)}")


def build_filter_expression(columns, values, operator, joiner):
    if isinstance(columns, str):
        columns = [columns]
    if isinstance(values, str):
        values = [values]
    expressions = []
    for column, value in zip(columns, values):
        if operator == 'contains':
            expressions.append(pl.col(column).contains(value))
        elif operator == 'startswith':
            expressions.append(pl.col(column).startswith(value))
        elif operator == 'endswith':
            expressions.append(pl.col(column).endswith(value))
        elif operator == 'match':
            expressions.append(pl.col(column).match(value))
        else:
            expressions.append(pl.col(column) == value)
    if joiner == 'and':
        return pl.expr.and_(*expressions)
    elif joiner == 'or':
        return pl.expr.or_(*expressions)
