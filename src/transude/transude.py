import pandas as pd
import polars as pl
from src.transude.pandas.data_frame_filter_factory import DataFrameFilterFactory
from src.transude.pandas.data_frame_query_builder import DataFrameQueryBuilder


def filter_df(data_frame, columns, values, operator, joiner='and'):
    if not isinstance(columns, list):
        columns = [columns]
    if not isinstance(values, list):
        values = [values]

    for column in columns:
        if column not in data_frame.columns:
            raise ValueError(f"Column '{column}' does not exist in DataFrame.")

    if isinstance(data_frame, pd.DataFrame):
        df_filters = DataFrameFilterFactory(columns=columns,
                                            values=values,
                                            operator=operator,
                                            joiner=joiner).create_filters()
        query = DataFrameQueryBuilder(df_filters).build_query()
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
