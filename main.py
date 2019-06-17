

import os 
import pandas as pd 
import numpy as np
import config 





##  This function converts string timestamps into datetime timestamps and store the new dataframe
def process_timestamp(df):
    string_timestamp_list = df.loc[:, 'timestamp']
    datetime_timestamp_list = list()
    for i in range(0, len(string_timestamp_list)):
        if i%100 == 0:
            print(i)
        datetime_timestamp_list.append(config.timestamp(string_timestamp_list[i]))
    datetime_timestamp_series = pd.Series(datetime_timestamp_list)
    df['timestamp_new'] = datetime_timestamp_series
    # storing the new dataframe
    df.to_csv(r'{}/training_new.csv'.format(os.getcwd()), index=False)
    return True 


## function for dividing the dataframe on basis of geolocation
def subset_dataframe_location_wise(df, folder_where_to_save):
    unique_locations = list(set(df['geohash6']))
    for i in range(0, len(unique_locations)):
        print(i)
        location_name = unique_locations[i]
        location_df = pd.DataFrame(df[df['geohash6']==location_name])
        location_df_path = '{}'.format(os.getcwd()) + '/{}'.format(folder_where_to_save) + '/{}.csv'.format(location_name)
        location_df.to_csv(location_df_path, index=False)
    return 


## Complete the location-wise data, so that each csv will have 4800 rows (96 timestamps for 50 days, so 96*50=4800)
def process_location_wise_dataframe(folder_path_where_location_data_is_saved):

    for file_name in os.listdir(folder_path_where_location_data_is_saved):

        if file_name == '.DS_Store':
            continue 
        else:
            location_name = file_name[:len(file_name)-4]
            location_file_path = r'{}/{}'.format(folder_path_where_location_data_is_saved, file_name)
            location_df = pd.read_csv(location_file_path, index_col=0)
            location_df = location_df.sort_values(['day', 'timestamp_new'], ascending=[True, True])
            for day in range(1, 51):
                print(day)
                location_time_list = list(location_df['timestamp'][location_df['day']==day])
                for time in config.timestamp_range:
                    if time in location_time_list:
                        continue
                    else:
                        row = {'geohash6':location_name, 'day':day, 'timestamp':time, 'demand':0, 'timestamp_new':config.timestamp(time)}
                        location_df = location_df.append(row, ignore_index=True)
            print(location_df.shape)
            break





def average_samples_for_each_class(dataframe):
    locations_samples_count_map = {}
    count = 0
    for i in set(dataframe['geohash6']):
        if count < 150:
            locations_samples_count_map[i] = dataframe[dataframe['geohash6']==i].shape[0]
            count +=1 
        else:
            break
    return locations_samples_count_map


