from db_connection import DBConnection

class WeatherStationDailyRecord:
    DB_DETAILS = {'db_name': 'my_weather_stations',
                  'user': 'd3x',
                  'password': 'd3x',
                  'host': '127.0.0.1',
                  'port': '5432'
                 }
    WEATHER_STATION_DAILY_RECORD = 'weather_station_daily_record'
    WEATHER_STATION_DAILY_RECORD_COLUMNS = """(weather_station_id, record_date,
                                               maximum_temperature, minimum_temperature,
                                               total_precipitation)
                                           """
    
    WEATHER_STATION_DAILY_RECORD_COLUMNS_LIST = ('weather_station_id',
                                                 'record_date',
                                                 'maximum_temperature',
                                                 'minimum_temperature',
                                                 'total_precipitation')
    
    WEATHER_STATION_DAILY_RECORD_SCHEMA = """
                                            (weather_station_id VARCHAR(30) NOT NULL,
                                             record_date DATE NOT NULL,
                                             maximum_temperature INTEGER,
                                             minimum_temperature INTEGER,
                                             total_precipitation INTEGER,
                                             PRIMARY KEY(weather_station_id, record_date)
                                             )
                                             """
        
    def __init__(self):
        self.db = None
        self.weather_station_id = None
        self.record_date = None
        self.maximum_temperature = None
        self.minimum_temperature = None
        self.total_precipitation = None
    
    # This method keeps the __init__ method clean to simplify unit testing
    def initialize(self):
        self.db = DBConnection(WeatherStationDailyRecord.DB_DETAILS)
        self.db.initialize()
        curs = self.db.get_cursor()
        WeatherStationDailyRecord.create_table(self.db)

    @classmethod 
    def create_table(cls, db_conn):
        create_table_command = f"""
                            CREATE TABLE IF NOT EXISTS {WeatherStationDailyRecord.WEATHER_STATION_DAILY_RECORD} 
                            {WeatherStationDailyRecord.WEATHER_STATION_DAILY_RECORD_SCHEMA};
                            """  
        try:
            curs = db_conn.get_cursor()
            curs.execute(create_table_command)
            db_conn.conn.commit()
        except Exception as e:
            print(f"Could not create table: {WeatherStationDailyRecord.WEATHER_STATION_DAILY_RECORD} -> {str(e)}")
    

    
class WeatherStationAnnualRecord:
    DB_DETAILS = {'db_name': 'my_weather_stations',
                  'user': 'd3x',
                  'password': 'd3x',
                  'host': '127.0.0.1',
                  'port': '5432'
                 }
    WEATHER_STATION_ANNUAL_RECORD = 'weather_station_annual_record'
    WEATHER_STATION_ANNUAL_RECORD_COLUMNS = """(weather_station_id, year,
                                                average_maximum_temperature, 
                                                average_minimum_temperature,
                                                total_precipitation)
                                            """
    
    WEATHER_STATION_ANNUAL_RECORD_COLUMNS_LIST = ('weather_station_id',
                                                  'year',
                                                  'average_maximum_temperature',
                                                  'average_minimum_temperature',
                                                  'total_precipitation')
    
    WEATHER_STATION_ANNUAL_RECORD_SCHEMA = """
                                            (weather_station_id VARCHAR(30) NOT NULL,
                                             year VARCHAR(4) NOT NULL,
                                             average_maximum_temperature DECIMAL,
                                             average_minimum_temperature DECIMAL,
                                             total_precipitation INTEGER,
                                             PRIMARY KEY(weather_station_id, year)
                                             )
                                          """
    
    def __init__(self):
        self.db = None
        self.weather_station_id = None
        self.year = None
        self.avg_max_temp = None
        self.avg_min_temp = None
        self.tot_precip = None
        
    # This method keeps the __init__ method clean to simplify unit testing
    def initialize(self):
        self.db = DBConnection(WeatherStationAnnualRecord.DB_DETAILS)
        self.db.initialize()
        curs = self.db.get_cursor()
        WeatherStationAnnualRecord.create_table(self.db)
        
    @classmethod 
    def create_table(cls, db_conn):
        create_table_command = f"""
                            CREATE TABLE IF NOT EXISTS {WeatherStationAnnualRecord.WEATHER_STATION_ANNUAL_RECORD} 
                            {WeatherStationAnnualRecord.WEATHER_STATION_ANNUAL_RECORD_SCHEMA};
                            """  
        try:
            curs = db_conn.get_cursor()
            curs.execute(create_table_command)
            db_conn.conn.commit()
        except Exception as e:
            print(f"Could not create table: {WeatherStationAnnualRecord.WEATHER_STATION_ANNUAL_RECORD} -> {str(e)}")
    
