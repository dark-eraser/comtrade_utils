import json
import time
from tqdm import tqdm
import logging
import os
import pandas as pd
from helper import *
import calendar
from dateutil.relativedelta import relativedelta
import csv
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_countries(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def fetch_data_for_all_countries(countries_file, period, cmd_code, flow_code, data_dir):
    countries = load_countries(countries_file)
    logger.info(f"Loaded {len(countries)} countries from {countries_file}")

    for country in tqdm(countries, desc="Fetching data for countries"):
        country_name = country['Country Name']
        reporter_code = country['Code']
        
        output_filename = f"{country_name.replace(' ', '_').lower()}_trade_data_{period}.json"
        
        logger.info(f"Fetching data for {country_name} (Code: {reporter_code})")
        
        try:
            json_file_path = fetch_comtrade_data(
                period=period,
                reporter_code=reporter_code,
                cmd_code=cmd_code,
                flow_code=flow_code,
                output_filename=output_filename,
                data_dir=data_dir
            )
            
            if json_file_path:
                logger.info(f"Successfully fetched data for {country_name}")
                
                df = structure_trade_data(json_file_path)
                if df is not None and not df.empty:
                    csv_filename = f"{country_name.replace(' ', '_').lower()}_trade_data.csv"
                    csv_path = os.path.join(data_dir, csv_filename)
                    
                    if os.path.exists(csv_path):
                        existing_df = pd.read_csv(csv_path)
                        combined_df = pd.concat([existing_df, df], ignore_index=True)
                        combined_df.to_csv(csv_path, index=False)
                        logger.info(f"Appended data to existing file: {csv_path}")
                    else:
                        df.to_csv(csv_path, index=False)
                        logger.info(f"Created new file with data: {csv_path}")
                else:
                    logger.warning(f"No data to structure for {country_name}")
            else:
                logger.warning(f"Failed to fetch data for {country_name}")
        
        except Exception as e:
            logger.error(f"Error processing {country_name}: {e}")
        
        # Add a delay to avoid overwhelming the API
        time.sleep(1)


# # Example usage
# if __name__ == "__main__":
#     countries_file = 'data/countries.json'
#     period = '202305'  # You can change the period
#     cmd_code = '91'  # Commodity code
#     flow_code = 'M'  # Flow code (M for imports)
#     data_dir = 'data/raw'

#     fetch_data_for_all_countries(countries_file, period, cmd_code, flow_code, data_dir)


def generate_monthly_periods(year):
    """Generate a list of monthly periods for a given year in the format YYYYMM."""
    start_date = date(year, 1, 1)
    periods = []
    for _ in range(12):
        periods.append(start_date.strftime("%Y%m"))
        start_date += relativedelta(months=1)
    print(periods)
    return periods

def fetch_data_for_all_countries_yearly(countries_file, year, cmd_code, flow_code, data_dir, product_name):
    countries = load_countries(countries_file)
    periods = generate_monthly_periods(year)

    for country in tqdm(countries, desc="Processing countries"):
        process_country_data(country, periods, cmd_code, flow_code, data_dir, product_name)


def process_country_data(country, periods, cmd_code, flow_code, data_dir, product_name):
    country_name = country['Country Name']
    reporter_code = country['Code']
    country_data = []

    for period in tqdm(periods, desc=f"Fetching months for {country_name}", leave=False):
        fetch_and_append_data(period, country_name, reporter_code, cmd_code, flow_code, data_dir, country_data)

    save_country_data(country_name, country_data, data_dir, product_name)


def fetch_and_append_data(period, country_name, reporter_code, cmd_code, flow_code, data_dir, country_data):
    output_filename = f"{country_name.replace(' ', '_').lower()}_trade_data_{period}.json"

    try:
        json_file_path = fetch_comtrade_data(
            period=period,
            reporter_code=reporter_code,
            cmd_code=cmd_code,
            flow_code=flow_code,
            output_filename=output_filename,
            data_dir=data_dir
        )

        if json_file_path:
            df = structure_trade_data(json_file_path)
            if df is not None and not df.empty:
                country_data.append(df)

    except Exception as e:
        tqdm.write(f"Error processing {country_name} for period {period}: {e}")

    time.sleep(1)


def save_country_data(country_name, country_data, data_dir, product_name):
    if country_data:
        combined_df = pd.concat(country_data, ignore_index=True)

        csv_filename = f"{country_name.replace(' ', '_').lower()}_{product_name.replace(' ', '_').lower()}_trade_data_{year}.csv"
        csv_path = os.path.join(data_dir, csv_filename)

        if os.path.exists(csv_path):
            existing_df = pd.read_csv(csv_path)
            final_df = pd.concat([existing_df, combined_df], ignore_index=True)
            final_df.to_csv(csv_path, index=False)
        else:
            combined_df.to_csv(csv_path, index=False)

        tqdm.write(f"Data for {country_name} saved to {csv_filename}")
    else:
        tqdm.write(f"No data collected for {country_name} for the entire year")

def fetch_data_for_all_products(countries_file, year, cmd_code, flow_code, data_dir, product_hs_nomenclature_file):
    with open(product_hs_nomenclature_file, 'r') as file:
        product_codes = json.load(file)
    
    for product_name, product_code in product_codes.items():
        tqdm.write(f"Fetching data for product: {product_name} (Code: {product_code})")
        fetch_data_for_all_countries_yearly(countries_file, year, product_code, flow_code, data_dir, product_name)

def fetch_data_for_all_products_for_country(period, flow_code, data_dir, product_hs_nomenclature_file, country_name):
    product_codes = {}
    with open(product_hs_nomenclature_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            product_code = row['ProductCode']
            product_name = row['Product Description']
            product_codes[product_name] = product_code
    
    for product_name, product_code in product_codes.items():
        tqdm.write(f"Fetching data for product: {product_name} (Code: {product_code}) for country: {country_name}")
        fetch_comtrade_data(period=period, reporter_code=country_name, cmd_code=product_code, flow_code=flow_code, output_filename=f"{country_name}_{product_name}.csv", data_dir=data_dir)

# Example usage
if __name__ == "__main__":
    # countries_file = 'data/countries.json'
    period = 202201  # You can change the year
    # cmd_code = '01'  # Commodity code
    flow_code = 'M'  # Flow code (M for imports)
    data_dir = 'data/raw'
    # product_name = "Watches"  # Replace with the actual product name
    # reporter_code="None",
    # fetch_data_for_all_countries_yearly(countries_file, year, cmd_code, flow_code, data_dir, product_name)
    country_name = "504"
    product_hs_nomenclature_file = 'data/products_hs_nomenclature.csv'
    fetch_data_for_all_products_for_country(period, flow_code, data_dir, product_hs_nomenclature_file, country_name)