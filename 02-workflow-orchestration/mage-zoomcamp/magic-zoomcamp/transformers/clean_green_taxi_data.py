if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    from functools import reduce
    import re
    import pandas as pd
    import numpy as np

    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    print("Before : ", data.columns, data.shape)
    print("The number of rides with zero passengers : ", data["passenger_count"].isin([0]).sum())
    print("The number of rides with zero trip distance : ", data["trip_distance"].isin([0]).sum())
    print("The number of rides with NaN passengers : ", data["passenger_count"].isna().sum())
    print("The number of rides with NaN trip distance : ", data["trip_distance"].isna().sum())
    print("The number of rides with negative trip distance : ", data[data["trip_distance"] < 0]["trip_distance"].count())
    print("The number of rides with negative passenger count : ", data[data["passenger_count"] < 0]["passenger_count"].count())

   
    print("Shape 1 : ", data[~(data["passenger_count"].isin([0]) | data["passenger_count"].isnull()) | (data["trip_distance"].isin([0] | data["trip_distance"].isnull() ))].shape)
    print("Shape 2: ", data[(data["passenger_count"] > 0) & (data["trip_distance"] > 0)].shape)

    '''
    # bins not working for unqual bins - NEED TO EXPLORE
    '''
    # bins = [0, 100, 20, 30, 40, 50, 60, 70, 80, 90, 100,10000, 12000, 14000, 16000, 20000, 25000,50000]
    # # print("Value counts as bin : ", data["trip_distance"].value_counts(bins=bins,sort=False))
    # print(pd.cut(data["trip_distance"], bins=bins).value_counts(sort=False))
   
    data.columns = (data.columns.str.replace(" ", "_").str.lower())
 
  
    data.rename(columns = {'vendorid':'vendor_id', 
                           'ratecodeid':'rate_code_id',
                           'pulocationid':'pu_location_id',
                           'dolocationid':'do_location_id'}, inplace = True)

    print(data.columns, data.shape)

    data["lpep_pickup_date"] = data["lpep_pickup_datetime"].dt.date
   
    print("Unique Vendor ID : ", data["vendor_id"].unique())
    print(data[data["vendor_id"].isnull()].head(2))
    return data[(data["passenger_count"] > 0) & (data["trip_distance"] > 0)]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    #assert output is not None, 'The output is undefined'
    print(output.shape)
    assert output["passenger_count"].isin([0]).sum() == 0 , 'There are still rides wirh zero passengers'
    assert output["trip_distance"].isin([0]).sum() == 0 , 'There are still wirh zero trip distance'
    assert "vendor_id" in output.columns, 'There is no column named "vendor_id"'