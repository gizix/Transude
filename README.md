# Transude
Simple tool set for filtering pandas DataFrames by building queries from one or more filters.  This is useful for connecting filtering controls on DataFrames using touch screen controls.
This is probably a re-invent of someone's better wheel but fun nonetheless.

ChatGPT's insight to the code:

    "It looks like the DataFrameFilter class is intended to be used as part of a query system for filtering rows in a DataFrame.
    
    A DataFrame is a two-dimensional size-mutable tabular data structure with rows and columns that can hold any data type. It is a fundamental data structure in the Python library pandas, which is widely used for data manipulation and analysis.
    
    The DataFrameFilter class allows users to create instances that represent a specific filter to apply to a DataFrame. The attributes of each DataFrameFilter instance specify the column to filter, the value to filter by, and the comparison operator to use.
    
    For example, a user could create a DataFrameFilter instance with the following attributes:
    
    column: "Name"
    value: "John"
    operator: "=="

    This DataFrameFilter instance would represent a filter that filters rows in a DataFrame based on whether the value in the "Name" column is equal to "John".
    
    The DataFrameFilter class also has several other optional attributes, such as in_use, joiner, filter_id, match_case, and regex, which allow users to specify additional details about how the filter should be applied.
    
    Overall, the DataFrameFilter class seems to be useful for creating filters to apply to a DataFrame as part of a query system, allowing users to easily specify the criteria for selecting specific rows in the DataFrame."

This project was developed with some consulting from ChatGPT. There were a few concepts I didn't understand until I had someone I could point more specific questions towards and see working examples.
Most of the scaffolding was written before consulting as it really tends to speed up responses and keep the conversation on track.  This is also a refactor of old code I use in a current project.
I also utilize a local TabNine model.  

Installation:

    Coming soon!
