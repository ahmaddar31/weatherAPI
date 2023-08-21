# import libraries
import pandas as pd
import requests
import json
import os


def fetch_data():

    
    url='https://api.openweathermap.org/data/2.5/forecast?'

    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the absolute path to config.json
    config_file = os.path.join(current_dir, "config.json")
    with open(config_file) as config:
        d_configs= json.load(config)

    api_key= d_configs["api_credentials"]["api_key"]
    city = d_configs["api_credentials"]["city"]

    try:
        # we get the response in a json format
        data = requests.get(url, params={"q":city,"appid":api_key})
        data.raise_for_status()
        weather = data.json()
        weather_list = weather["list"]

        # Create an empty list to store individual weather data as dictionaries
        weather_data_list = []

        # Iterate through each weather entry and extract the required data
        for weather_entry in weather_list:
            dt_txt = weather_entry["dt_txt"]
            temp = weather_entry["main"]["temp"]
            feels_like = weather_entry["main"]["feels_like"]
            temp_min = weather_entry["main"]["temp_min"]
            temp_max = weather_entry["main"]["temp_max"]
            pressure = weather_entry["main"]["pressure"]
            humidity = weather_entry["main"]["humidity"]
            weather_description = weather_entry["weather"][0]["description"]
            wind_speed = weather_entry["wind"]["speed"]
            wind_deg = weather_entry["wind"]["deg"]

            # Append the extracted data as a dictionary to the list
            weather_data_list.append({
                "Date-Time": dt_txt,
                "Temperature": temp,
                "Feels Like": feels_like,
                "Min Temperature": temp_min,
                "Max Temperature": temp_max,
                "Pressure": pressure,
                "Humidity": humidity,
                "Weather Description": weather_description,
                "Wind Speed": wind_speed,
                "Wind Degree": wind_deg
            })

        # Create a DataFrame from the list of weather data
        weather_df = pd.DataFrame(weather_data_list)
        df1 = pd.json_normalize(weather)
        #axis=1 (column), axis=0 (row)
        df1= df1.drop("list",axis=1)

        #concatinate the two dataframes and convert it to csv file
        new_df= pd.concat([df1,weather_df], ignore_index=False, axis=1)# ignore_index to re-index the columns
        new_df.fillna(method="ffill", axis=0)
        new_df.to_csv("data.csv", index=False)
        csv_file = r'data.csv'
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file)
        df_filled= df.fillna(method="ffill", axis=0)
        df_filled.to_csv("data.csv", index=False)

    except exceptions as e:
        if requests.exceptions.RequestException:
            if data.status_code == 400 :
                print('Bad request')
            elif data.status_code == 401:
                print('Unauthorized check your credentials')
            elif data.status_code == 404:
                print('not found check your api')
            else:
                print(requests.exceptions.RequestException)
        else:
            print(e)
fetch_data()