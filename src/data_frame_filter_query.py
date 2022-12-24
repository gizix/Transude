from typing import List
from ChatGPT.data_frame_filter_no_docstrings import DataFrameFilter


class DataFrameFilterQuery:
    """
    This class acts as a group of DataFrameFilters and
    constructs a full query from each filter based on their attributes.
    """
    def __init__(self, data_frame_filters: List[DataFrameFilter] = None, joiner_overwrite: str = None):
        self.data_frame_filters = data_frame_filters
        self.joiner_overwrite = joiner_overwrite
        if data_frame_filters is None:
            self.data_frame_filters = []

    def __repr__(self):
        """
        Return a string representation of the object.
        :return: A string representation of the object.
        """
        return f"<{self.__class__.__name__}: filters={self.data_frame_filters}>"

    def add_filter(self, dataframe_filter: DataFrameFilter) -> None:
        """
        Add a filter to the list of filters.
        :param dataframe_filter: The filter to add.
        """
        self.data_frame_filters.append(dataframe_filter)

    def add_unique_filter(self, dataframe_filter: DataFrameFilter) -> None:
        """
        Add a filter to the list of filters.
        :param dataframe_filter: The filter to add.
        """
        if dataframe_filter not in self.data_frame_filters:
            self.data_frame_filters.append(dataframe_filter)

    def remove_filter(self, dataframe_filter: DataFrameFilter) -> bool:
        """
        Remove a filter from the list of filters.
        :param dataframe_filter: The filter to remove.
        :return: True if the filter was removed, False if not.
        """
        if not self.data_frame_filters:
            return False
        else:
            try:
                self.data_frame_filters.remove(dataframe_filter)
                return True
            except ValueError as exc:
                exc.add_note(f"{__name__}: Did not find {dataframe_filter!r} to remove.")
                return False

    def remove_filter_by_index(self, index: int) -> bool:
        """
        Remove a filter from the list of filters.
        :param index: The index of the filter to remove.
        :return: True if the filter was removed, False if not.
        """
        try:
            self.data_frame_filters.pop(index)
            return True
        except (IndexError, ValueError) as exc:
            exc.add_note(f"{__name__}: Did not find index={index!r} to remove.")
            return False

    def remove_non_search_non_toggled_filter_by_index(self, index: int) -> bool:
        """
        Removes a non-searching, non-toggled filter from the filters list by index.
        :param index: The index of the filter to be removed.
        :return: True if the filter was removed, False if the filter was not found.
        """
        for i, filter_ in enumerate(self.data_frame_filters):
            if i == index and not filter_.is_searching and not filter_.is_toggled:
                self.data_frame_filters.pop(i)
                return True
        return False

    def remove_non_search_filter_by_index(self, index: int) -> bool:
        """
        Removes a non-searching, non-toggled filter from the filters list by index.
        :param index: The index of the filter to be removed.
        :return: True if the filter was removed, False if the filter was not found
        """
        if not self.data_frame_filters:
            return False
        else:
            non_global_pop_index = 0
            for dataframe_filter in self.data_frame_filters:
                if dataframe_filter.is_searching:
                    continue
                if non_global_pop_index == index:
                    self.data_frame_filters.remove(dataframe_filter)
                    return True
                non_global_pop_index += 1
        return False

    def remove_search_filters(self) -> bool:
        """
        Removes all search filters from the list of filters.
        :return: True if the filter was removed, False if not.
        """
        removed = False
        for filter_ in self.data_frame_filters:
            if filter_.is_searching:
                self.data_frame_filters.remove(filter_)
                removed = True
        return removed

    def clear_all(self) -> None:
        """
        Remove all filters from the list of filters.
        """
        self.data_frame_filters.clear()

    def get_full_query(self, skip_toggled: bool = False) -> str:
        """
        Get the full query string.
        :param skip_toggled: If True, skip toggled filters.
        :return: The full query string.
        """
        if not self.data_frame_filters:
            return ""
        query_parts = []
        search_filters = []
        for dataframe_filter in self.data_frame_filters:
            if skip_toggled and dataframe_filter.is_toggled:
                continue
            query = dataframe_filter.get_query()
            if not query:
                continue
            joiner_to_use = " & " if dataframe_filter.joiner is None else f" {dataframe_filter.joiner} "
            if self.joiner_overwrite:
                joiner_to_use = f" {self.joiner_overwrite} "
            if dataframe_filter.is_searching:
                search_filters.append((joiner_to_use, f"({query})"))
            else:
                if query_parts:
                    query_parts.append(f"{joiner_to_use}({query})")
                else:
                    query_parts.append(f"({query})")
        for joiner, query in search_filters:
            if query_parts:
                query_parts.append(f"{joiner}({query})")
            else:
                query_parts.append(f"({query})")
        return "".join(query_parts)
