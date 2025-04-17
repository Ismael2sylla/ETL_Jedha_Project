# SuperCourier - Mini ETL Pipeline
# Starter code for the Data Engineering mini-challenge

import sqlite3
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
import random
import os

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('supercourier_mini_etl')

# Constants
DB_PATH = 'supercourier_mini.db'
WEATHER_PATH = 'weather_data.json'
OUTPUT_PATH = 'deliveries.csv'

# 1. FUNCTION TO GENERATE SQLITE DATABASE
def create_sqlite_database():
    logger.info("Creating SQLite database...")
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE deliveries (
        delivery_id INTEGER PRIMARY KEY,
        pickup_datetime TEXT,
        package_type TEXT,
        delivery_zone TEXT,
        recipient_id INTEGER
    )
    ''')
    package_types = ['Small', 'Medium', 'Large', 'X-Large', 'Special']
    delivery_zones = ['Urban', 'Suburban', 'Rural', 'Industrial', 'Shopping Center']
    deliveries = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    for i in range(1, 1001):
        timestamp = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        package_type = random.choices(package_types, weights=[25, 30, 20, 15, 10])[0]
        delivery_zone = random.choice(delivery_zones)
        deliveries.append((
            i,
            timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            package_type,
            delivery_zone,
            random.randint(1, 100)
        ))
    cursor.executemany('INSERT INTO deliveries VALUES (?, ?, ?, ?, ?)', deliveries)
    conn.commit()
    conn.close()
    logger.info(f"Database created with {len(deliveries)} deliveries")
    return True

# 2. FUNCTION TO GENERATE WEATHER DATA
def generate_weather_data():
    logger.info("Generating weather data...")
    conditions = ['Sunny', 'Cloudy', 'Rainy', 'Windy', 'Snowy', 'Foggy']
    weights = [30, 25, 20, 15, 5, 5]
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    weather_data = {}
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        weather_data[date_str] = {}
        for hour in range(24):
            if hour > 0 and random.random() < 0.7:
                condition = weather_data[date_str].get(str(hour-1), random.choices(conditions, weights=weights)[0])
            else:
                condition = random.choices(conditions, weights=weights)[0]
            weather_data[date_str][str(hour)] = condition
        current_date += timedelta(days=1)
    with open(WEATHER_PATH, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)
    logger.info(f"Weather data generated for period {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
    return weather_data

# 3. EXTRACTION FUNCTIONS
def extract_sqlite_data():
    logger.info("Extracting data from SQLite database...")
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM deliveries"
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Extraction complete: {len(df)} records")
    return df

def load_weather_data():
    logger.info("Loading weather data...")
    with open(WEATHER_PATH, 'r', encoding='utf-8') as f:
        weather_data = json.load(f)
    logger.info(f"Weather data loaded for {len(weather_data)} days")
    return weather_data

# 4. TRANSFORMATION FUNCTIONS
def enrich_with_weather(df, weather_data):
    logger.info("Enriching with weather data...")
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    def get_weather(timestamp):
        date_str = timestamp.strftime('%Y-%m-%d')
        hour_str = str(timestamp.hour)
        try:
            return weather_data[date_str][hour_str]
        except KeyError:
            return None
    df['WeatherCondition'] = df['pickup_datetime'].apply(get_weather)
    return df

def transform_data(df_deliveries, weather_data):
    logger.info("Transforming data...")
    df = enrich_with_weather(df_deliveries, weather_data)
    df['Weekday'] = df['pickup_datetime'].dt.day_name()
    df['Hour'] = df['pickup_datetime'].dt.hour
    df['Distance'] = np.round(np.random.uniform(1, 50, size=len(df)), 2)
    df['Actual_Delivery_Time'] = df['Distance'] * np.random.uniform(0.8, 1.5, size=len(df)) + 30
    df['Actual_Delivery_Time'] = df['Actual_Delivery_Time'].round(2)

    pkg_factors = {
        'Small': 1, 'Medium': 1.2, 'Large': 1.5, 'X-Large': 2, 'Special': 2.5
    }
    zone_factors = {
        'Urban': 1.2, 'Suburban': 1, 'Rural': 1.3, 'Industrial': 0.9, 'Shopping Center': 1.4
    }
    weather_factors = {
        'Sunny': 1, 'Cloudy': 1.05, 'Rainy': 1.2, 'Windy': 1.1, 'Snowy': 1.8, 'Foggy': 1.3
    }
    df['Base_Theoretical_Time'] = 30 + df['Distance'] * 0.8
    df['Adjustment_Factor'] = df['package_type'].map(pkg_factors) * \
                               df['delivery_zone'].map(zone_factors) * \
                               df['WeatherCondition'].map(weather_factors).fillna(1)
    df['Adjusted_Time'] = df['Base_Theoretical_Time'] * df['Adjustment_Factor']
    df['Delay_Threshold'] = df['Adjusted_Time'] * 1.2
    df['Status'] = np.where(df['Actual_Delivery_Time'] > df['Delay_Threshold'], 'Delayed', 'On-time')

    df = df.dropna()

    final_columns = [
        'delivery_id', 'pickup_datetime', 'Weekday', 'Hour', 'package_type', 'Distance',
        'delivery_zone', 'WeatherCondition', 'Actual_Delivery_Time', 'Status'
    ]
    df = df[final_columns]
    df.columns = ['Delivery_ID', 'Pickup_DateTime', 'Weekday', 'Hour', 'Package_Type',
                  'Distance', 'Delivery_Zone', 'Weather_Condition', 'Actual_Delivery_Time', 'Status']
    return df

# 5. LOADING FUNCTION
def save_results(df):
    logger.info("Saving results...")
    logger.info(f"Number of records: {len(df)}")
    logger.info(f"On-time deliveries: {(df['Status'] == 'On-time').sum()}")
    logger.info(f"Delayed deliveries: {(df['Status'] == 'Delayed').sum()}")
    df.to_csv(OUTPUT_PATH, index=False)
    logger.info(f"Results saved to {OUTPUT_PATH}")
    return True

# MAIN FUNCTION
def run_pipeline():
    try:
        logger.info("Starting SuperCourier ETL pipeline")
        create_sqlite_database()
        weather_data = generate_weather_data()
        df_deliveries = extract_sqlite_data()
        df_transformed = transform_data(df_deliveries, weather_data)
        save_results(df_transformed)
        logger.info("ETL pipeline completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error during pipeline execution: {str(e)}")
        return False

if __name__ == "__main__":
    run_pipeline()
