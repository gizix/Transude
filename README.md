# Transude
Simple tool set for filtering DataFrames (Pandas or ~~Polars~~*) by building queries from one or more filters.  This is useful for connecting filtering controls on DataFrames using touch screen controls.

This project was developed with some consulting from ChatGPT. There were a few concepts I didn't understand until I had someone I could point more specific questions towards and see working examples.
Most of the scaffolding was written before consulting as it really tends to speed up responses and keep the conversation on track.  This is also a refactor of old code I use in a current project.

Installation:

    pip install transude

Usage:
    
    import pandas as pd
    import polars as pl
    import transude as txd
    
    # Create a DataFrame using Pandas or Polars
    pd_df = pd.DataFrame(...)
    pl_df = pl.DataFrame(...)
    
    # Get a filtered version of the DataFrame using Transude
    filtered_pd_df = txd.filter_df(data_frame=pd_df, columns='col1', values=['val1', 'val2'], operator='==', joiner='or')

    filtered_pl_df = txd.filter_df(data_frame=pl_df, columns='col2', values=['val1'], operator='<')

If you need to manage the DataFrameFilters directly, you can use a DataFrameFilterQueryBuilder like so:

    pd_query_builder = DataFrameQueryBuilder()
    
    # Example of adding a single DataFrameFilter and clearing the filters.  Filters can be removed one by one as well.
    pd_query_builder.add_filter(DataFrameFilter(columns='col1', values='val1', operator='==', joiner='or'))
    pd_query_builder.clear_filters()

    # The following utilizes the DataFrameFilterFactory to create multiple filters and then adds them all to the builder.
    pd_filter_factory = DataFrameFilterFactory(columns='col1', values=['val1', 'val2'], operator='==', joiner='or')
    pd_filters = pd_filter_factory.create_filters()
    pd_query_builder.add_filters(pd_filters)
    query_string = pd_query_builder.build_query()

    # In order to apply the filters, call query using the query_string
    pd_df.query(query_string)

--*Polars compatability coming soon.