import pandas
from _snowflake import vectorized


@vectorized(input=pandas.DataFrame)
def add_inputs(df):
    return df[0] + df[1]
