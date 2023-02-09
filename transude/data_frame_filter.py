import datetime
import itertools
import pandas as pd


class DataFrameFilter:
    """
    This class represents part of a DataFrame query.
    """
    next_filter_id = itertools.count()

    def __init__(self, column: str,
                 value: str | int | float | bool | datetime.datetime | pd.Timestamp,
                 operator: str,
                 in_use: bool = True,
                 joiner: str = None,
                 filter_id: int = None,
                 match_case: bool = False,
                 regex: bool = False,
                 data_frame: pd.DataFrame = None,
                 omit_on_clear: bool = False,
                 common_name: str = None,
                 group_joiner: str = None):
        """
        Initializes a DataFrameFilter instance.

        :param column: str
        Name of the column in the DataFrame.
        :param value: str
        Value to filter by.
        :param operator: str
        Comparison operator to use.
        :param in_use: bool (default: True)
        Whether this filter is in use or not.
        :param joiner: str (default: None)
        How to join this filter with other filters in the query.
        :param filter_id: int (default: None)
        ID of this filter.
        :param match_case: bool (default: False)
        Whether to match the case of the string value.
        :param regex: bool (default: False)
        Whether the value is a regular expression.
        :param data_frame: pd.DataFrame (default: None)
        DataFrame to filter.
        :param omit_on_clear: bool (default: False)
        Option to omit this filter when clearing all filters.
        :param common_name: str (default: None)
        Specified common description of the filter.
        :param group_joiner: str (default: None)
        How to join this filter (and others that share the same filter_id) with other filters in the query.
        """
        if not DataFrameFilter.is_valid_operator(operator):
            raise ValueError(f"Invalid operator: {operator}")
        if joiner is None:
            joiner = "and"
        if filter_id is None:
            filter_id = next(DataFrameFilter.next_filter_id) + 1
        if group_joiner is None:
            group_joiner = '&'
        self.column = column
        self.value = value
        self.operator = operator
        self.in_use = in_use
        self.joiner = joiner
        self.filter_id = filter_id
        self.match_case = match_case
        self.regex = regex
        self.data_frame = data_frame
        self.omit_on_clear = omit_on_clear
        self.common_name = common_name
        self.group_joiner = group_joiner

    def __repr__(self) -> str:
        """
        Returns a string representation of this DataFrameFilter instance.
        """
        return f"DataFrameFilter(column='{self.column}', value='{self.value}', " \
               f"operator='{self.operator}', joiner='{self.joiner}', filter_id={self.filter_id}, " \
               f"match_case={self.match_case}, regex={self.regex}, data_frame={self.data_frame.__str__()}, " \
               f"omit_on_clear={self.omit_on_clear}, common_name={self.common_name}, group_joiner={self.group_joiner})"

    def __str__(self) -> str:
        """
        Returns the query string for this DataFrameFilter instance.
        """
        return self.get_query() if self.common_name is None else self.common_name

    @staticmethod
    def is_valid_str_operator(operator: str) -> bool:
        """
        Returns whether the given operator is a valid string operator.

        :param operator: str
        The operator to check.

        :return: bool
        """
        valid_str_operators = ['contains', 'startswith', 'endswith', 'match']
        return operator in valid_str_operators

    @staticmethod
    def is_valid_non_str_operator(operator: str) -> bool:
        """
        Returns whether the given operator is a valid non-string operator.

        :param operator: str
        The operator to check.

        :return: bool
        """
        valid_non_str_operators = ['==', '>', '<', '>=', '<=', '!=']
        return operator in valid_non_str_operators

    @staticmethod
    def is_valid_operator(operator: str) -> bool:
        """
        Returns whether the given operator is a valid operator.

        :param operator: str
        The operator to check.

        :return: bool
        """
        return DataFrameFilter.is_valid_str_operator(operator) or DataFrameFilter.is_valid_non_str_operator(operator)

    def get_query(self) -> str:
        """
        Returns the query string for this DataFrameFilter.

        :return: str
        The query string for this filter.
        """
        if DataFrameFilter.is_valid_str_operator(self.operator):  # constructing a str operation query
            value_clause = f"{repr(self.value)}, case={self.match_case}, regex={self.regex}"
            if self.operator != "contains":
                value_clause = f"{repr(self.value)}"
            if self.data_frame is not None and self.data_frame[self.column].dtype.name == 'string':
                return f"{self.column}.str.{self.operator}({value_clause})"
            return f"{self.column}.astype('str').str.{self.operator}({value_clause})"
        return f"{self.column} {self.operator} {repr(self.value)}"
