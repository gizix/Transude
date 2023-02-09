import pandas as pd
import datetime
from typing import Union, List
from .data_frame_filter import DataFrameFilter

ValueSingleTyping = str | int | float | bool | datetime.datetime | pd.Timestamp
ValueMultiTyping = Union[Union[ValueSingleTyping, List[str]], Union[ValueSingleTyping, List[int]],
                         Union[ValueSingleTyping, List[float]], Union[ValueSingleTyping, List[bool]],
                         Union[ValueSingleTyping, List[datetime.datetime]]]


class DataFrameFilterFactory:
    """
    This class is used to create a list of properly formed DataFrameFilter instances given
    any columns, any values, a valid operator, and optional parameters.
    """
    def __init__(self,
                 columns: Union[str, List[str]],
                 values: ValueMultiTyping,
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
        :param data_frame: pd.DataFrame (default: None)
        The DataFrame to filter.
        :param omit_on_clear: bool (default: False)
        Option to omit these filters when clearing all filters.
        :param common_name: str (default: None)
        Specified common description of the filters.
        :param group_joiner: str (default: None)
        How these filters should join with other filters in the query.
        """
        if not DataFrameFilter.is_valid_operator(operator):
            raise ValueError(f"Invalid operator: {operator}")
        if joiner is None:
            joiner = "and"
        if group_joiner is None:
            group_joiner = '&'
        self.columns = columns
        self.values = values
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

    def create_filters(self) -> List[DataFrameFilter]:
        """
        Creates a list of `DataFrameFilter` instances.

        :return: List[DataFrameFilter]
        """
        if not isinstance(self.values, list):
            self.values = [self.values]
        if not isinstance(self.columns, list):
            self.columns = [self.columns] * len(self.values)

        is_string_filter = DataFrameFilter.is_valid_str_operator(self.operator)

        if self.data_frame is not None and not is_string_filter:
            filters = []
            for column, value in zip(self.columns, self.values):
                # Get the data type of the column
                dtype = self.data_frame[column].dtype.name
                if dtype == 'int64':
                    value = int(value)
                elif dtype == 'float64':
                    value = float(value)
                elif dtype == 'datetime64[ns]':
                    value = pd.to_datetime(value)
                elif dtype == 'object':
                    value = str(value)
                filters.append(DataFrameFilter(column=column,
                                               value=value,
                                               operator=self.operator,
                                               in_use=self.in_use,
                                               joiner=self.joiner,
                                               filter_id=self.filter_id,
                                               data_frame=self.data_frame,
                                               omit_on_clear=self.omit_on_clear,
                                               common_name=self.common_name,
                                               group_joiner=self.group_joiner))
            return filters
        else:
            return [DataFrameFilter(column=column,
                                    value=str(value),
                                    operator=self.operator,
                                    in_use=self.in_use,
                                    joiner=self.joiner,
                                    filter_id=self.filter_id,
                                    match_case=self.match_case,
                                    regex=self.regex,
                                    data_frame=self.data_frame,
                                    omit_on_clear=self.omit_on_clear,
                                    common_name=self.common_name,
                                    group_joiner=self.group_joiner)
                    for column, value in zip(self.columns, self.values)]
