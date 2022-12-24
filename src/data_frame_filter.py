import datetime
from abc import ABC, abstractmethod
from typing import Union, ClassVar, List, TypeAlias

"""Type Aliases used for readability when type hinting DataFrameFilter subclass __init__ calls."""
_SingleOrListType: TypeAlias = Union[str, List[str]]
_SingleOrMultiValueType: TypeAlias = Union[Union[str, List[str]], Union[str, List[int]], Union[str, List[float]],
                                           Union[str, List[bool]], Union[str, List[datetime.date]]]
_SingleValueType: TypeAlias = Union[str, int, float, bool, datetime.date]
_MultiValueType: TypeAlias = Union[List[str], List[int], List[float], List[bool], List[datetime.date]]


class DataFrameFilter(ABC):
    """
    This class constructs part of a query for filtering a pandas.DataFrame using a DataFrameFilterQuery.
    Filters are generally in two different formats:

    Precise Matching
        'column operator value'
        i.e.:
            COL_1 == 0.0    (dtype='float')
                or
            COL_2 > 44      (dtype='int')

    Partial Matching (for use with search)
        'column.astype('str').str.contains(value)'
        i.e.:
            COL_1.astype('str').str.contains('{value}', case=match_case, regex=regex)
            *NOTE* Columns of dtype str do not need the explicit conversion in the query.
    """

    """Class variables:"""
    columns: ClassVar[_SingleOrListType]  # The column portion of the query
    dtypes: ClassVar[_SingleOrListType]  # String construction based on datatypes of the values
    operator: ClassVar[str]  # The operator portion of the query
    values: ClassVar[_SingleOrMultiValueType]  # Value portion of the query
    match_case: ClassVar[bool] = False
    regex: ClassVar[bool] = False
    searching: ClassVar[bool] = False
    toggled: ClassVar[bool] = False
    force_empty_value: ClassVar[bool] = False
    joiner: ClassVar[str] = None  # For joining multiple DataFrameFilters together in DataFrameFilterQuery

    def __init__(self, **kwargs):
        """
        Initialize the object's attributes with the keyword arguments passed to the constructor.
        :param kwargs: Keyword arguments for initializing the object's attributes.
        """
        self.__dict__.update(kwargs)

    def __repr__(self) -> str:
        """
        Return a string representation of the object.
        :return: String representation of the object.
        """
        kwarg_str = ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}: {kwarg_str}>"

    @property
    def is_searching(self) -> bool:
        """
        Return whether the filter is being used for a search.
        :return: Boolean indicating whether the filter is being used for a search.
        """
        return self.searching

    @property
    def is_toggled(self) -> bool:
        """
        Return whether the filter is currently enabled.
        :return: Boolean indicating whether the filter is currently enabled.
        """
        return self.toggled

    @abstractmethod
    def get_query(self) -> str:
        """
        Return the filter's query as a string.
        :return: The filter's query as a string.
        """
        pass


class DataFrameFilterFactory(ABC):
    """
    Factory for constructing the proper DataFrameFilter type for the given parameters.
    """
    @classmethod
    def create_data_frame_filter(cls, **kwargs) -> DataFrameFilter:
        """
        Create a new DataFrameFilter object based on the given keyword arguments.
        :param kwargs: Keyword arguments for initializing the DataFrameFilter object.
        :return: A new DataFrameFilter object.
        """
        columns = kwargs.get("columns")
        dtypes = kwargs.get("dtypes")
        operator = kwargs.get("operator")
        values = kwargs.get("values")
        match_case = kwargs.get("match_case")
        regex = kwargs.get("regex")
        search = kwargs.get("search")
        toggled = kwargs.get("toggled")
        force_empty_value = kwargs.get("force_empty_value")
        joiner = kwargs.get("joiner")

        if isinstance(columns, str) and not isinstance(values, List):
            """single column + single value"""
            kwargs.update({
                'column': columns,
                'dtype': dtypes,
                'value': values
            })
            return SingleColumnSingleValueFilter(**kwargs)
        elif isinstance(columns, str) and isinstance(values, List):
            """single column + list of values"""
            kwargs.update({
                'column': columns,
                'dtype': dtypes,
                'values': values
            })
            return SingleColumnMultiValueFilter(**kwargs)
        elif isinstance(columns, List) and isinstance(values, str):
            """list of columns + single value"""
            kwargs.update({
                'columns': columns,
                'dtype': dtypes,
                'value': values
            })
            return MultiColumnSingleValueFilter(**kwargs)
        elif isinstance(columns, List) and isinstance(values, List):
            """list of columns + list of values"""
            kwargs.update({
                'columns': columns,
                'dtype': dtypes,
                'values': values
            })
            return MultiColumnMultiValueFilter(**kwargs)


class SingleColumnSingleValueFilter(DataFrameFilter):
    """
    Filter for a single column with a single value.
    """
    def __init__(self, column: str, dtype: str, value: _SingleValueType, **kwargs):
        """
        Initialize the object's attributes with the given parameters and any additional keyword arguments.
        :param column: Name of the column to filter.
        :param dtype: Data type of the column.
        :param value: Value to filter with.
        :param kwargs: Additional keyword arguments for initializing the object's attributes.
        """
        kwargs.update({
            'columns': [column],
            'dtypes': [dtype],
            'values': [value]
        })
        super().__init__(**kwargs)

    def get_query(self) -> str:
        """
        Return the filter query as a string.
        :return: The filter query as a string.
        """
        column, dtype, value, = self.columns[0], self.dtypes[0], self.values[0]
        if self.is_searching:
            if dtype in ['string']:
                return f"{column}.str.{self.operator}({repr(value)}, case={self.match_case}, regex={self.regex}"
            return f"{column}.astype('str').str.{self.operator}({repr(value)}, case={self.match_case}, regex={self.regex}"
        if dtype in ['int', 'float', 'bool']:
            # for integer, float, and boolean types, no quotes are needed around the value
            return f"{column} {self.operator} {value}"
        else:
            # for all other types, quotes are needed around the value
            return f"{column} {self.operator} '{value}'"


class SingleColumnMultiValueFilter(DataFrameFilter):
    """
    Filter for a single column with multiple values.
    """
    def __init__(self, column: str, dtype: str, values: _MultiValueType, **kwargs):
        """
        Initialize the object's attributes with the given parameters and any additional keyword arguments.
        :param column: Name of the column to filter.
        :param dtype: Data type of the column.
        :param values: List of values to filter with.
        :param kwargs: Additional keyword arguments for initializing the object's attributes.
        """
        kwargs.update({
            'columns': [column],
            'dtypes': [dtype],
        })
        super().__init__(columns=column, dtypes=dtype, values=values, **kwargs)

    def get_query(self) -> str:
        """
        Return the filter query as a string.
        :return: The filter query as a string.
        """
        column, dtype = self.columns[0], self.dtypes[0]
        if self.searching:
            if dtype in ['string']:
                return f"{self.columns}.str.contains('{self.values}', case={self.match_case}, regex={self.regex})"
            return f"{self.columns}.astype('str').str.contains('{self.values}', case={self.match_case}, regex={self.regex})"
        else:
            return f"{self.columns} {self.operator} {self.values}"


class MultiColumnSingleValueFilter(DataFrameFilter):
    """
    Filter for multiple columns with a single value.
    """
    def __init__(self, columns: List[str], dtypes: List[str], value: _SingleValueType, **kwargs):
        """
        Initialize the object's attributes with the given parameters and any additional keyword arguments.
        :param columns: Names of the columns to filter.
        :param dtypes: Data types of the columns.
        :param value: Value to filter with.
        :param kwargs: Additional keyword arguments for initializing the object's attributes.
        """
        kwargs.update({
            'columns': columns,
            'dtypes': dtypes,
            'values': [value]
        })
        super().__init__(columns=columns, dtypes=dtypes, values=value, **kwargs)

    def get_query(self) -> str:
        """
        Return the filter query as a string.
        :return: The filter query as a string.
        """
        value = self.values[0]
        if self.searching:
            queries = []
            for column, dtype in zip(self.columns, self.dtypes):
                if dtype in ['string']:
                    queries.append(f"{column}.str.contains('{value}', case={self.match_case}, regex={self.regex})")
                else:
                    queries.append(
                        f"{column}.astype('str').str.contains('{value}', case={self.match_case}, regex={self.regex})")
            return " OR ".join(queries)  # join queries with "or" operator
        else:
            return f"{' OR '.join(self.columns)} {self.operator} {value}"


class MultiColumnMultiValueFilter(DataFrameFilter):
    """
    Filter for multiple columns with multiple values.
    """
    def __init__(self, columns: List[str], dtypes: List[str], values: _MultiValueType, **kwargs):
        """
        Initialize the object's attributes with the given parameters and any additional keyword arguments.
        :param columns: Names of the columns to filter.
        :param dtypes: Data types of the columns.
        :param values: Values to filter with.
        :param kwargs: Additional keyword arguments for initializing the object's attributes.
        """
        super().__init__(columns=columns, dtypes=dtypes, values=values, **kwargs)

    def get_query(self) -> str:
        """
        Return the filter query as a string.
        :return: The filter query as a string.
        """
        if self.searching:
            queries = []
            for column, dtype, value in zip(self.columns, self.dtypes, self.values):
                if dtype in ['string']:
                    queries.append(f"{column}.str.contains('{value}', case={self.match_case}, regex={self.regex})")
                else:
                    queries.append(
                        f"{column}.astype('str').str.contains('{value}', case={self.match_case}, regex={self.regex})")
            return " OR ".join(queries)  # join queries with "or" operator
        else:
            queries = []
            for column, value in zip(self.columns, self.values):
                queries.append(f"{column} {self.operator} {value}")
            return " & ".join(queries)  # join queries with "and" operator
