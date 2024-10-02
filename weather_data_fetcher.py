import os
import pandas as pd
import numpy as np
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Define the top 10 largest cities in Catalonia with their coordinates
cities = [
    {"name": "Barcelona", "latitude": 41.3851, "longitude": 2.1734},
    {"name": "L'Hospitalet de Llobregat", "latitude": 41.3662, "longitude": 2.1169},
    {"name": "Badalona", "latitude": 41.4500, "longitude": 2.2474},
    {"name": "Terrassa", "latitude": 41.5610, "longitude": 2.0089},
    {"name": "Sabadell", "latitude": 41.5433, "longitude": 2.1094},
    {"name": "Lleida", "latitude": 41.6176, "longitude": 0.6200},
    {"name": "Tarragona", "latitude": 41.1189, "longitude": 1.2445},
    {"name": "Mataró", "latitude": 41.5381, "longitude": 2.4445},
    {"name": "Santa Coloma de Gramenet", "latitude": 41.4515, "longitude": 2.2080},
    {"name": "Reus", "latitude": 41.1498, "longitude": 1.1055},
]

# Define the weather variables you want to fetch
weather_variables = [
    "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
    "precipitation", "rain", "snowfall", "snow_depth", "weather_code", "pressure_msl",
    "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
    "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_100m",
    "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m", "soil_temperature_0_to_7cm",
    "soil_temperature_7_to_28cm", "soil_temperature_28_to_100cm", "soil_temperature_100_to_255cm",
    "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm", "soil_moisture_28_to_100cm",
    "soil_moisture_100_to_255cm"
]

def fetch_and_process_data(url, params, city_name):
    try:
        response = openmeteo.weather_api(url, params=params)[0]
        hourly = response.Hourly()
        
        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )
        }
        
        for idx, var_name in enumerate(weather_variables):
            hourly_data[var_name] = hourly.Variables(idx).ValuesAsNumpy()
        
        return pd.DataFrame(data=hourly_data)
    except Exception as e:
        print(f"Error fetching data for {city_name}: {e}")
        return None

def update_csv(new_data, csv_file):
    if os.path.exists(csv_file):
        existing_data = pd.read_csv(csv_file, parse_dates=['date'])
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        combined_data.drop_duplicates(subset='date', inplace=True)
        combined_data.sort_values('date', inplace=True)
        combined_data.reset_index(drop=True, inplace=True)
    else:
        combined_data = new_data
    
    combined_data.to_csv(csv_file, index=False)

def process_city(city, fetch_archive=True):
    city_name = city['name']
    csv_file = f"data/{city_name.replace(' ', '_')}.csv"
    
    if fetch_archive:
        today = datetime.now().date()
        if os.path.exists(csv_file):
            existing_data = pd.read_csv(csv_file, parse_dates=['date'])
            start_date = existing_data['date'].max().date() + timedelta(days=1)
        else:
            start_date = today - timedelta(days=30)  # Default to fetching last 30 days if no existing data
        
        if (today - start_date).days < 2:
            print(f"Skipping archive data fetch for {city_name} as start date is within last 48 hours.")
            return

        params = {
            "latitude": city['latitude'],
            "longitude": city['longitude'],
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": (today - timedelta(days=2)).strftime('%Y-%m-%d'),
            "hourly": weather_variables,
            "timezone": "UTC"
        }
        url = "https://archive-api.open-meteo.com/v1/archive"
    else:
        params = {
            "latitude": city['latitude'],
            "longitude": city['longitude'],
            "hourly": weather_variables,
            "timezone": "UTC",
            "past_days": 2,
            "forecast_days": 0
        }
        url = "https://api.open-meteo.com/v1/forecast"

    new_data = fetch_and_process_data(url, params, city_name)
    if new_data is not None:
        update_csv(new_data, csv_file)
        print(f"Data for {city_name} updated successfully.")

# Main execution
if not os.path.exists('data'):
    os.makedirs('data')

for city in cities:
    process_city(city, fetch_archive=True)

print("All cities processed for archive data.")

for city in cities:
    process_city(city, fetch_archive=False)

print("All cities processed with last 48 hours data.")