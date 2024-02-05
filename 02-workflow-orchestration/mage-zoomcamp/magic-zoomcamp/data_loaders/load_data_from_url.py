import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_url(*args, **kwargs):
    """
    Template for loading data from API
    """
    # url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    
    url_path = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"
    url_files = ["green_tripdata_2020-10.csv.gz", "green_tripdata_2020-11.csv.gz", 
    "green_tripdata_2020-12.csv.gz"]

    df_all = pd.DataFrame()

    for f in url_files:

        file_path = url_path + f
        print(file_path)

        taxi_dtypes = {
                        'VendorID': pd.Int64Dtype(),
                        'passenger_count': pd.Int64Dtype(),
                        'trip_distance': float,
                        'RatecodeID':pd.Int64Dtype(),
                        'store_and_fwd_flag':str,
                        'PULocationID':pd.Int64Dtype(),
                        'DOLocationID':pd.Int64Dtype(),
                        'payment_type': pd.Int64Dtype(),
                        'fare_amount': float,
                        'extra':float,
                        'mta_tax':float,
                        'tip_amount':float,
                        'tolls_amount':float,
                        'improvement_surcharge':float,
                        'total_amount':float,
                        'congestion_surcharge':float
                    }

        # native date parsing 
        parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

        df_temp = pd.read_csv(
        file_path, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        print(f"file path : {file_path} : rows and columns : {df_temp.shape}")

        df_all=pd.concat([df_all,df_temp],ignore_index=True)


    print(df_all.shape)
    # print(df_all.head(10))
    # print(df_all.tail(10))
    # print(df_all.columns)
    return df_all


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
