import itertools
from datetime import datetime
from typing import List, Self, Union


class DataFrameFilter:
    """
    This class represents part of a DataFrame query.
    """
    def __init__(self, column: str, value: str, operator: str,
                 in_use: bool = True, joiner: str = None, filter_id: int = None,
                 match_case: bool = False, regex: bool = False):
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
        """
        if not DataFrameFilter.is_valid_operator(operator):
            raise ValueError(f"Invalid operator: {operator}")
        if joiner is None:
            joiner = "and"
        self.column = column
        self.value = value
        self.operator = operator
        self.in_use = in_use
        self.joiner = joiner
        self.filter_id = filter_id
        self.match_case = match_case
        self.regex = regex

    def __repr__(self) -> str:
        """
        Returns a string representation of this DataFrameFilter instance.
        """
        return f"DataFrameFilter(column='{self.column}', value='{self.value}', " \
               f"operator='{self.operator}', joiner='{self.joiner}', filter_id={self.filter_id}, " \
               f"match_case={self.match_case}, regex={self.regex})"

    def __str__(self) -> str:
        """
        Returns the query string for this DataFrameFilter instance.
        """
        return self.get_query()

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
        valid_non_str_operators = ['==', '>', '<', '>=', '<=']
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
            value_clause = f"{repr(self.value)}"
            if self.match_case or self.regex:
                value_clause = f"{repr(self.value)}, case={self.match_case}, regex={self.regex}"
            return f"{self.column}.str.{self.operator}({value_clause})"
        return f"{self.column} {self.operator} {repr(self.value)}"


class DataFrameFilterFactory:
    """
    This class is used to create a list of properly formed DataFrameFilter instances given
    any columns, any values, a valid operator, and optional parameters.
    """
    next_filter_id = itertools.count()

    def __init__(self,
                 columns: Union[str, List[str]],
                 values: Union[Union[str, List[str]], Union[str, List[int]], Union[str, List[float]],
                               Union[str, List[bool]], Union[str, List[datetime.date]]],
                 operator: str,
                 in_use: bool = True,
                 joiner: str = None,
                 filter_id: int = None,
                 match_case: bool = False,
                 regex: bool = False):
        """
        Initializes a DataFrameFilterFactory instance.

        :param columns: Union[str, List[str]]
        Name(s) of the column(s) in the DataFrame.
        :param values: Union[Union[str, List[str]], Union[str, List[int]], Union[str, List[float]],
        Union[str, List[bool]], Union[str, List[datetime.date]]]
        Value(s) to filter by.
        :param operator: str
        Comparison operator to use.
        :param in_use: bool (default: True)
        Whether these filters are in use or not.
        :param joiner: str (default: None)
        How to join these filters with other filters in the query.
        :param filter_id: int (default: None)
        ID of these filters.
        :param match_case: bool (default: False)
        Whether to match the case of the string value(s).
        :param regex: bool (default: False)
        Whether the value(s) is/are regular expressions.
        """
        if not DataFrameFilter.is_valid_operator(operator):
            raise ValueError(f"Invalid operator: {operator}")
        if joiner is None:
            joiner = "and"
        if filter_id is None:
            filter_id = next(DataFrameFilterFactory.next_filter_id) + 1
        self.columns = columns
        self.values = values
        self.operator = operator
        self.in_use = in_use
        self.joiner = joiner
        self.filter_id = filter_id
        self.match_case = match_case
        self.regex = regex

    def create_filters(self) -> List[DataFrameFilter]:
        """
        Creates a list of `DataFrameFilter` instances.

        :return: List[DataFrameFilter]
        """
        if not isinstance(self.values, list):
            self.values = [self.values]
        if not isinstance(self.columns, list):
            self.columns = [self.columns] * len(self.values)
        return [DataFrameFilter(column=column, value=str(value), operator=self.operator,
                                joiner=self.joiner, filter_id=self.filter_id,
                                match_case=self.match_case, regex=self.regex)
                for column, value in zip(self.columns, self.values)]


class DataFrameQueryBuilder:
    """
    This class constructs proper string queries using DataFrameFilters for use with the pandas.DataFrame.query() method.
    """

    def __init__(self, data_frame_filters: List[DataFrameFilter] = None):
        """
        Initializes a DataFrameQueryBuilder instance.

        :param data_frame_filters: List[DataFrameFilter] (default: None)
        List of DataFrameFilters to use.

        :var data_frame_filters: List[DataFrameFilter]
        The DataFrameFilters to use in building the query.
        """
        if data_frame_filters is None:
            data_frame_filters = []
        self._data_frame_filters = data_frame_filters

    def __repr__(self) -> str:
        return f"DataFrameQueryBuilder(data_frame_filters={self.data_frame_filters})"

    @property
    def data_frame_filters(self) -> List[DataFrameFilter]:
        return self._data_frame_filters

    def add_filter(self, data_frame_filter: DataFrameFilter) -> Self:
        """
        Add a DataFrameFilter to the list of filters.

        :param data_frame_filter: DataFrameFilter
        The DataFrameFilter to add.
        :return: self
        """
        self.data_frame_filters.append(data_frame_filter)
        return Self

    def remove_filter(self, data_frame_filter: DataFrameFilter) -> Self:
        """
        Remove a DataFrameFilter from the list of filters.

        :param data_frame_filter: DataFrameFilter
        The DataFrameFilter to remove.
        :return: self
        :raises ValueError: if the given DataFrameFilter is not in the list of filters.
        """
        try:
            self.data_frame_filters.remove(data_frame_filter)
            return Self
        except ValueError as exc:
            exc.add_note(f"Could not remove {data_frame_filter} from {self!r}")
            raise exc

    def remove_filter_by_index(self, index: int) -> Self:
        """
        Remove a DataFrameFilter from the list of filters by index.

        :param index: int
        The index of the DataFrameFilter to remove.
        :return: self
        :raises IndexError: if the given index is out of bounds.
        """
        try:
            del self.data_frame_filters[index]
            return Self
        except IndexError as exc:
            exc.add_note(f"Could not remove DataFrameFilter at index={index} from {self!r}")
            raise exc

    def remove_filters_by_id(self, filter_id: int) -> Self:
        """
        Remove DataFrameFilters from the list of filters by filter ID.

        :param filter_id: int
        The filter ID of the DataFrameFilters to remove.
        :return: self
        :raises ValueError: if no DataFrameFilters with the given filter ID are found.
        """
        if not any(df_filter.filter_id == filter_id for df_filter in self.data_frame_filters):
            raise ValueError(f"Filter with id {filter_id} not found")
        self._data_frame_filters = [df_filter for df_filter in self.data_frame_filters if
                                    df_filter.filter_id != filter_id]
        return Self

    def clear_filters(self) -> Self:
        """
        Clear all DataFrameFilters from the list of filters.

        :return: self
        """
        self.data_frame_filters.clear()
        return Self

    def disable_filters(self) -> Self:
        """
        Disable all DataFrameFilters in the list of filters.

        :return: self
        """
        for df_filter in self.data_frame_filters:
            df_filter.in_use = False
        return Self

    def disable_filters_by_id(self, filter_id: int) -> Self:
        """
        Disable DataFrameFilters with the given filter ID.

        :param filter_id: int
        The filter ID of the DataFrameFilters to disable.
        :return: self
        """
        for df_filter in self.data_frame_filters:
            if df_filter.filter_id == filter_id:
                df_filter.in_use = False
        return Self

    def enable_filters(self) -> Self:
        """
        Enable all DataFrameFilters in the list of filters.

        :return: self
        """
        for df_filter in self.data_frame_filters:
            df_filter.in_use = True
        return Self

    def enable_filters_by_id(self, filter_id: int) -> Self:
        """
        Enable DataFrameFilters with the given filter ID.

        :param filter_id: int
        The filter ID of the DataFrameFilters to enable.
        :return: self
        """
        for df_filter in self.data_frame_filters:
            if df_filter.filter_id == filter_id:
                df_filter.in_use = True
        return Self

    def build_query(self) -> str:
        """
        Build a proper string query using the DataFrameFilters in the list of filters.

        :return: str
        The constructed query.
        """
        query = ''
        for df_filter in self.data_frame_filters:
            if not df_filter.in_use:
                continue
            if not query:
                query = f"({df_filter.get_query()})"
                continue
            query += f" {df_filter.joiner} ({df_filter.get_query()})"
        return query
