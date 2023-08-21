import pyodbc as odbc
import pandas as pd
import os
import json
import sqlalchemy

def load_data():

    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the absolute path to config.json
    config_file = os.path.join(current_dir, "config.json")
    with open(config_file) as config:
        d_configs= json.load(config)

    # Set up the database connection
    driver = d_configs['sqlserver_credentials']['driver']
    server = d_configs['sqlserver_credentials']['server']
    database = d_configs['sqlserver_credentials']['database']
    username = d_configs['sqlserver_credentials']['username']
    password = d_configs['sqlserver_credentials']['password']
    
    connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};uid={username};pwd={password}"

    # Establish the connection
    try:
        conn = odbc.connect(connection_string)
        # Connection successful, you can perform database operations here
        print("Connection successful!")
        csv_file = r'data.csv'
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file)
        df_columns = df.columns.tolist()

        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()
        # Insert the data from the DataFrame into the SQL Server table
        for index, row in df.iterrows():
            insert_query = f"""
            INSERT INTO weatherData (cod,message,cnt,city_id,Name,lat,lon,country,population,timezone,sunrise,sunset,
            Date_time,tempreture,Feels_like,temp_min,temp_max,pressure,humidity,Description,wind_speed,wind_degree)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """
            cursor.execute(insert_query, row.values.tolist())
        # Commit the changes
        conn.commit()

        # Close the cursor and the database connection
        cursor.close()
        conn.close()
    except exceptions as e:
        if (odbc.Error):
            # Error occurred while connecting
            print(f"Error connecting to the database: {str(odbc.Error)}")
        else:
            print(e)
load_data()