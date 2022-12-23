from typing import List

from src.data_frame_filter import DataFrameFilter


class DataFrameFilterQuery:
    """
    This class acts as a group of DataFrameFilters and
    constructs a full query from each filter based on their attributes.
    """
    def __init__(self):
        self.filters: List[DataFrameFilter] = []

    def __repr__(self):
        return f"<{self.__class__.__name__}: filters={self.filters}>"

    def add_filter(self, dataframe_filter: DataFrameFilter) -> None:
        self.filters.append(dataframe_filter)

    def add_unique_filter(self, dataframe_filter: DataFrameFilter) -> None:
        if dataframe_filter not in self.filters:
            self.filters.append(dataframe_filter)

    def rem_filter(self, dataframe_filter: DataFrameFilter) -> bool:
        if not self.filters:
            return False
        try:
            self.filters.remove(dataframe_filter)
        except ValueError as exc:
            exc.add_note(f"{__name__}: Did not find {dataframe_filter!r} to remove.")
            return False
        return True

    def rem_filter_by_index(self, index: int) -> bool:
        try:
            self.filters.pop(index)
            return True
        except (IndexError, ValueError) as exc:
            exc.add_note(f"{__name__}: Did not find index={index!r} to remove.")
            return False

    def rem_non_search_non_toggled_filter_by_index(self, index: int) -> bool:
        non_global_non_toggled = [df_filter for df_filter in self.filters if
                                  not df_filter.is_searching and not df_filter.is_toggled]
        for i, dataframe_filter in enumerate(non_global_non_toggled):
            if i == index:
                self.filters.remove(dataframe_filter)
                return True
        return False

    def rem_non_search_filter_by_index(self, index: int) -> bool:
        if not self.filters:
            return False
        else:
            non_global_pop_index = 0
            for dataframe_filter in self.filters:
                if dataframe_filter.is_searching:
                    continue
                if non_global_pop_index == index:
                    self.filters.remove(dataframe_filter)
                    return True
                non_global_pop_index += 1
        return False

    def remove_search_filters(self):
        """
        Remove all search filters.
        """
        filters = [f for f in self.filters if f.is_searching]
        if filters:
            for f in filters:
                self.filters.remove(f)
            return True
        return False

    def clear_all(self) -> None:
        self.filters.clear()

    def get_full_query(self, skip_toggled: bool = False) -> str:
        if not self.filters:
            return ""
        query_parts = []
        search_filters = []
        for dataframe_filter in self.filters:
            if skip_toggled and dataframe_filter.is_toggled:
                continue
            query = dataframe_filter.get_query()
            if not query:
                continue
            joiner_to_use = dataframe_filter.joiner or "&"
            if dataframe_filter.is_searching:
                search_filters.append((joiner_to_use, query))
            else:
                if query_parts:
                    query_parts.append(f"{joiner_to_use}{query}")
                else:
                    query_parts.append(query)
        for joiner, query in search_filters:
            if query_parts:
                query_parts.append(f"{joiner}{query}")
            else:
                query_parts.append(query)
        return "".join(query_parts)
