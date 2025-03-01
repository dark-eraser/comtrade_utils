import json
import pandas as pd
import comtradeapicall
from datetime import date, timedelta
import logging
import os
from tqdm import tqdm
import time
import csv
from dateutil.relativedelta import relativedelta
import concurrent.futures  # Import for parallel processing

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def fetch_comtrade_data(
    period,
    reporter_code,
    cmd_code,
    flow_code,
    output_filename,
    data_dir,
    type_code='C',
    freq_code='M',
    cl_code='HS',
    partner_code=None,
    partner2_code=None,
    customs_code=None,
    mot_code=None,
    max_records=500,
    format_output='JSON',
    aggregate_by=None,
    breakdown_mode='classic',
    count_only=None,
    include_desc=True,
    max_retries=3 
):
    """
    Fetches trade data from the UN Comtrade API and appends it to a CSV file.
    """
    try:
        os.makedirs(data_dir, exist_ok=True)
        full_filename = os.path.join(data_dir, output_filename)
        df = comtradeapicall.previewFinalData(
            typeCode=type_code, freqCode=freq_code, clCode=cl_code, period=period,
            reporterCode=reporter_code, cmdCode=cmd_code, flowCode=flow_code,
            partnerCode=partner_code, partner2Code=partner2_code, customsCode=customs_code,
            motCode=mot_code, maxRecords=max_records, format_output=format_output,
            aggregateBy=aggregate_by, breakdownMode=breakdown_mode, countOnly=count_only,
            includeDesc=include_desc
        )

        if isinstance(df, pd.DataFrame):
            if not df.empty:
                df['productCode'] = cmd_code
                df['refPeriod'] = period

                columns = [
                    "productCode", "refPeriod", "reporterDesc", "partnerDesc", "flowDesc",
                    "cmdDesc", "cifvalue", "fobvalue", "primaryValue", "qty", "netWgt", "grossWgt"
                ]
                df = df[columns]

                number_cols = ["cifvalue", "fobvalue", "primaryValue", "qty", "netWgt", "grossWgt"]
                for col in number_cols:
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "")

                df.to_csv(full_filename, mode='a', header=not os.path.isfile(full_filename), index=False)

                logger.info(f"Data fetched and appended to: {full_filename} for the period {period}")
                return full_filename
            else:
                logger.warning(f"Empty DataFrame returned for cmd_code: {cmd_code}")
            time.sleep(0.5)
        else:
            logger.warning(f"Unexpected data type returned: {type(df)}")
    except Exception as e:
        logger.error(f"Error fetching data: {e}")


def get_country_code(country_name, countries):
    """
    Get the country code for a given country name.
    """
    for country in countries:
        if country['Country Name'] == country_name:
            return country['Code']
    return None

def load_countries(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def fetch_data_for_all_countries(countries_file, period, cmd_code, flow_code, data_dir):
    countries = load_countries(countries_file)
    logger.info(f"Loaded {len(countries)} countries from {countries_file}")

    with tqdm(countries, desc="Fetching data for countries") as country_tqdm:
        for country in country_tqdm:
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
            
            time.sleep(1)

def generate_monthly_periods(year):
    """Generate a list of monthly periods for a given year in the format YYYYMM."""
    start_date = date(year, 1, 1)
    periods = []
    for _ in range(12):
        periods.append(start_date.strftime("%Y%m"))
        start_date += relativedelta(months=1)
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
    """
    Fetches trade data from the Comtrade API for a given period and country, processes it, 
    and appends it to the provided country_data list.

    Args:
        period (str): The time period for which to fetch trade data.
        country_name (str): The name of the country for which to fetch trade data.
        reporter_code (str): The reporter code for the country.
        cmd_code (str): The commodity code for the trade data.
        flow_code (str): The flow code (import/export) for the trade data.
        data_dir (str): The directory where the fetched data will be stored.
        country_data (list): A list to which the processed trade data DataFrame will be appended.

    Raises:
        Exception: If there is an error during the fetching or processing of the trade data.

    Returns:
        None
    """
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
    
def structure_trade_data(json_file_path):
    """
    Reads the JSON file containing the trade data, structures it into a DataFrame, and returns it.
    """
    try:
        with open(json_file_path, 'r') as file:
            data = json.load(file)
            if 'dataset' in data:
                df = pd.DataFrame(data['dataset'])
                return df
            else:
                return None
    except Exception as e:
        tqdm.write(f"Error structuring data: {e}")
        return None

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
    
    for product_name, product_code in tqdm(product_codes.items(), desc="Fetching data for products"):
        tqdm.write(f"Fetching data for product: {product_name} (Code: {product_code})")
        fetch_data_for_all_countries_yearly(countries_file, year, product_code, flow_code, data_dir, product_name)

def fetch_data_for_all_products_for_country(periods, flow_code, data_dir, product_hs_nomenclature_file, country_name, country_code):
    product_codes = {}
    flow = 'imports' if flow_code=='M' else 'exports'
    year = periods[0][:4]
    with open(product_hs_nomenclature_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['Tier'] == '2':
                product_code = row['ProductCode']
                product_name = row['Product Description']
                product_codes[product_name] = product_code
    for product_name, product_code in product_codes.items():
        for period in periods:
            tqdm.write(f"Fetching data for product: {product_name} (Code: {product_code}) for country: {country_name}")
            fetch_comtrade_data(period=period, reporter_code=country_code, cmd_code=product_code, flow_code=flow_code, output_filename=f"{country_name}_{year}_{flow}.csv", data_dir=data_dir)
