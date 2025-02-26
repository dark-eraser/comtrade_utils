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


# Example usage
if __name__ == "__main__":
    # countries_file = 'data/countries.json'
    year = 2022  # You can change the year
    # cmd_code = '01'  # Commodity code
    flow_code = 'M'  # Flow code (M for imports)
    data_dir = 'data/raw'
    # product_name = "Watches"  # Replace with the actual product name
    # reporter_code="None",
    # fetch_data_for_all_countries_yearly(countries_file, year, cmd_code, flow_code, data_dir, product_name)
    country_name = "MAR"
    countries = load_countries('countries.json')
    country_code = get_country_code(country_name, countries)
    product_hs_nomenclature_file = 'products_hs_nomenclature.csv'
    periods = generate_monthly_periods(year)
    fetch_data_for_all_products_for_country(periods, flow_code, data_dir, product_hs_nomenclature_file, country_name, country_code)