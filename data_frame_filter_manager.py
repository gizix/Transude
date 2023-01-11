from typing import Self, List
from data_frame_filter import DataFrameFilter


class DataFrameFilterManager:
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

    def add_filters(self, data_frame_filters: List[DataFrameFilter]) -> Self:
        """
        Add a list of DataFrameFilters to the list of filters.

        :param data_frame_filters: List[DataFrameFilter]
        The DataFrameFilters to add
        :return: self
        """
        for df_filter in data_frame_filters:
            self.data_frame_filters.append(df_filter)
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
