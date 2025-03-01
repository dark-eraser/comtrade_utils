import json
import time
import logging
import os
from helper import *
import calendar
import csv
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    year = 2022  # You can change the year
    flow_code = 'M'  # Flow code (M for imports)
    data_dir = 'data/raw'
    country_name = "MAR"
    countries = load_countries('countries.json')
    country_code = get_country_code(country_name, countries)
    product_hs_nomenclature_file = 'products_hs_nomenclature.csv'
    periods = generate_monthly_periods(year)
    fetch_data_for_all_products_for_country(periods, flow_code, data_dir, product_hs_nomenclature_file, country_name, country_code)