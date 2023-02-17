import pandas as pd # great for data manipulation
import os # used to play with git repository clone
import psycopg2 # postgresql/python connector
from datetime import datetime
from weather_station_records import WeatherStationDailyRecord, WeatherStationAnnualRecord

def create_master_file():
	# Inside 'code-challenge-template/src' directory so below variables are relative
    weather_records_directory = '../wx_data/'
    answers_directory = '../answers/'
    master_file = answers_directory + 'master.csv'

    # Open master file to populate with data from all repository files	
    with open(master_file, 'w') as w1:
	    for file in os.listdir(weather_records_directory):
	        filename = os.path.join(weather_records_directory, file)
	        #print(filename)
	        if os.path.isfile(filename):
	            # Use the file's name to identify weather stations
	            weather_station_code = os.path.splitext(file)[0] 
	            with open(filename, 'r') as r1:
	                for line in r1:
	                    date, max_temp, min_temp, precip_total = line.split()
	                    yyyy = date[:4] # Needed to construct postgresql date format 
	                    mm = date[4:6] # Needed to construct postgresql date format 
	                    dd = date[6:] # Needed to construct postgresql date format 

                        # Format and populate the master file csv 
	                    master_file_row = f"{weather_station_code}|{yyyy}-{mm}-{dd}|{max_temp}|{min_temp}|{precip_total}"
	                    #print(master_file_row)
	                    w1.write(f"{master_file_row}\n")
    return master_file

def create_dataframe(master_file, headers, delimiter):
    df = pd.read_csv(master_file, sep=delimiter, names=headers)	
    return df

def clean_9999(column_to_clean):
    cleaned_column = None if '-9999' in column_to_clean else int(column_to_clean)
    return cleaned_column

    
def main():
    master_file = create_master_file()
	# Populate new Pandas dataframe with masterfile rows 
    headers = ['Weather Station Code', 'Date', 'Max Temperature', 'Minimum temperature', 'Precipitation']
    weather_df = create_dataframe(master_file, headers, '|')


    try:
        weather_station = WeatherStationDailyRecord()
        weather_station.initialize()
        #logging.debug(weather_station.__dict__)
    except Exception as e:
        print(f"Error(s) when working with WeatherStationDailyRecord object: {str(e)}")
    
    undigested_records = [] # list of records that could not be inserted into the db table
    start_ingest_time = datetime.now() # Start populating db
    with open(master_file, 'r') as r1:
	    for line in r1:
	        weather_station_code, date, max_temp, min_temp, precip = line.split('|')
	        #According to the assignment, 'Missing values are indicated by the value -9999.'
	        # Convert value to None so that Pandas can convert these values to NaN.
	       
            # Clean -9999 data 
	        max_temp = clean_9999(max_temp)
	        min_temp = clean_9999(min_temp)
	        precip = clean_9999(precip)
	        #min_temp = None if '-9999' in min_temp else int(min_temp)
	        #precip = None if '-9999' in precip else int(precip)
	        
	        row_to_insert = (weather_station_code, date, max_temp, min_temp, precip)
	        row_to_insert_placeholder = '(%s, %s, %s, %s, %s)'
	        #   {WeatherStationRecord.MIDWEST_STATION_COLUMNS} 
	        insert_query = f"""
	                        INSERT INTO 
	                        {WeatherStationDailyRecord.WEATHER_STATION_DAILY_RECORD}
	                        VALUES
	                        {row_to_insert_placeholder};
	                        """
	        #print(insert_query)
	        try:
	            cursor = weather_station.db.get_cursor()
	            cursor.execute(insert_query, row_to_insert)
	        except Exception as e:
	            # print(f"Error while inserting into db: {str(e)}")
	            undigested_records.append(line) # Keep a list of records that could not be added
	
	# Commit all inserts made above to the postgresql table outside of for loop
	# Purposely kept out of the for loop to improve performance when working with larger datasets
	# A batch function can be used within the for loop to find a sweet spot (performance vs quicker persistence to disk)
    weather_station.db.conn.commit() 
    end_ingest_time = datetime.now() # End populating db
    if undigested_records:
	#    logging.error(f"Lines that could not be ingested: {undigested_records}"))
        print(f"Lines that could not be ingested: {undigested_records}")
    
    print(f"Started Table Population @ {start_ingest_time}")
    print(f"Ended Table Population @ {end_ingest_time}")
    duration = end_ingest_time - start_ingest_time
    print(f"Table Population Duration: {duration}")	
	
	# Get dataframe from database
    select_query = f"SELECT * FROM {WeatherStationDailyRecord.WEATHER_STATION_DAILY_RECORD}"
    weather_df = weather_station.db.postgresql_to_dataframe(select_query,
	                                                        WeatherStationDailyRecord.WEATHER_STATION_DAILY_RECORD_COLUMNS_LIST)
	# Add new column, 'year', using the first 4 digits of the date
    weather_df['year'] = weather_df.record_date.astype(str).str[:4]

    # If a row's 3 calcuable columns are Null, then remove the row
    weather_df = weather_df.dropna(subset=['maximum_temperature', 'minimum_temperature', 'total_precipitation'], how='all')

    # For each weather station, the average of its (non-Nan) columns is calculated using either the 'mean' or 'sum' functions with 'groupby' and stored in a new dataframe
    renamed_columns = {'maximum_temperature': 'average_maximum_temperature',
	                   'minimum_temperature': 'average_minimum_temperature',
	                   'total_precipitation':'total_precipitation'}
	
    weather_calculated_df = weather_df.groupby(['weather_station_id', 'year']).agg({'maximum_temperature':'mean',
	                                                        'minimum_temperature':'mean',
	                                                        'total_precipitation':'sum'
	                                                       }).rename(columns=renamed_columns).reset_index()
    # Take derived dataframe and populate the new table in the DB
    try:
        weather_station = WeatherStationAnnualRecord()
        weather_station.initialize()
	    #logging.debug(weather_station.__dict__)
    except Exception as e:
        print(f"Error(s) when working with WeatherStationAnnualRecord object: {str(e)}")
	
    start_ingest_time = datetime.now()
    weather_station.db.dataframe_to_postgresql(weather_calculated_df,
	                                           WeatherStationAnnualRecord.WEATHER_STATION_ANNUAL_RECORD)
    end_ingest_time = datetime.now()
    print(f"Started Table Population @ {start_ingest_time}")
    print(f"Ended Table Population @ {end_ingest_time}")
    duration = end_ingest_time - start_ingest_time
    print(f"Table Population Duration: {duration}")


if __name__ == "__main__":
    main()
