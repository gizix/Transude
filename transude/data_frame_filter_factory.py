import itertools
from datetime import datetime
from typing import Union, List
from .data_frame_filter import DataFrameFilter


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
