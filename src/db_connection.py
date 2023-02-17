import psycopg2 # postgresql/python connector
import pandas as pd

class DBConnection:

    def __init__(self, db_details):
        self.db_name = db_details.get('db_name')
        self.user = db_details.get('user')
        self.password = db_details.get('password')
        self.host = db_details.get('host')
        self.port = db_details.get('port')
        self.conn = None        
    
    # This method keeps the __init__ method clean to simplify unit testing
    def initialize(self):
        self.connect_to_db() 
        
    def connect_to_db(self):
        try:
            self.conn = psycopg2.connect(dbname = self.db_name,
                                            user = self.user,
                                            password = self.password,
                                            host = self.host,
                                            port = self.port
                                           )
        except Exception as e:
            print('Uh oh! We could not connect to the db with the following details:'
                  f"{DB_DETAILS} -> {str(e)}")

    # Method ensures new cursors are created for executed queries.
    # This ensures that cache is not used        
    def get_cursor(self):
        try:
            curs = self.conn.cursor()
        except Exception as e:
            print(f"Could not retrieve cursor to {self.db_name} -> {str(e)}")
        return curs

    
    def postgresql_to_dataframe(self, select_query, column_names=[]):
        
        try:
            cursor = self.get_cursor()
            cursor.execute(select_query)
        except Exception as e:
            print(f"Uh oh. Could not import the table data into a dataframe: {str(e)}")
            cursor.close()
            return

        rows = cursor.fetchall()
        cursor.close()

        df = pd.DataFrame(rows, columns=column_names)
        return df
    
    def dataframe_to_postgresql(self, data_frame, tbl):
        try:
            from sqlalchemy import create_engine
            engine = create_engine(f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}")
            data_frame.to_sql(tbl, engine, if_exists='replace', index = False)
        except Exception as e:
            print(f"Uh oh! Could not import the dataframe into table {tbl}: {str(e)}")
