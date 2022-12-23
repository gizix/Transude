# Transude
Simple tool set for filtering pandas DataFrames by building queries from one or more filters.  This is useful for connecting filtering controls on DataFrames using touch screen controls.
This is probably a reinvent of someone's better wheel but fun nonetheless.

ChatGPT's insight to the code:

"This is a code for creating a class DataFrameFilter which serves as a base class for constructing part of a query for filtering a pandas DataFrame. The DataFrameFilter class has several class variables and methods, including:

columns: The column portion of the query. This is a type alias for either a single string or a list of strings.
dtypes: String construction based on the datatypes of the values. This is a type alias for either a single string or a list of strings.
operator: The operator portion of the query. This is a string.
values: The value portion of the query. This is a type alias for a union of various types, including strings, lists of strings, lists of integers, lists of floats, lists of booleans, and lists of datetime dates.
match_case: A boolean indicating whether or not to match case when searching. This is set to False by default.
regex: A boolean indicating whether or not to use regex when searching. This is set to False by default.
searching: A boolean indicating whether the filter is being used for a search. This is set to False by default.
toggled: A boolean indicating whether the filter is currently enabled. This is set to False by default.
force_empty_value: A boolean indicating whether to force the value to be empty. This is set to False by default.
joiner: A string for joining multiple DataFrameFilters together in a DataFrameFilterQuery.
The DataFrameFilter class also has an __init__ method which initializes the object's attributes with the keyword arguments passed to the constructor, as well as a __repr__ method which returns a string representation of the object.

The DataFrameFilter class has an abstract method get_query which returns the filter's query as a string.

The code also includes a DataFrameFilterFactory class which serves as a factory for constructing the proper DataFrameFilter type for the given parameters. The DataFrameFilterFactory class has a class method create_data_frame_filter which creates a new DataFrameFilter object based on the given keyword arguments.

Overall, it seems that this code is intended to provide a framework for creating and working with filters for pandas DataFrames."

This project was developed with some consulting from ChatGPT. There were a few concepts I didn't understand until I had someone I could point more specific questions towards and see working examples.
Most of the scaffolding was written before consulting as it really tends to speed up responses and keep the conversation on track.  This is also a refactor of old code I use in a current project.
I also utilize a local TabNine model that really helps to fill in what OpenAI starts.  
