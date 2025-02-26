# Comtrade Utils

This repository contains a Python-based tool for fetching and analyzing international trade data from the UN Comtrade API. The tool automates the process of retrieving, structuring, and storing trade data for various countries and products.

## Features

-   **Data Fetching**:
    -   Fetches trade data from the UN Comtrade API for specified countries, products, and time periods.
    -   Supports imports (`flow_code = 'M'`) and exports.
    -   Handles API rate limits and retries failed requests.
-   **Data Processing**:
    -   Structures the raw JSON data into pandas DataFrames.
    -   Formats large numbers with commas for better readability.
    -   Allows selection and reordering of columns for customized analysis.
-   **Data Storage**:
    -   Saves processed data into CSV files, organized by country and product.
    -   Appends new data to existing files or creates new files if they don't exist.
-   **Configuration**:
    -   Uses JSON files (`countries.json`, `products_hs_nomenclature.csv`) for configurable country and product lists.
    -   Allows specifying time periods (yearly with monthly breakdowns) for data retrieval.
-   **Logging**:
    -   Includes comprehensive logging to track data fetching progress, errors, and warnings.
    -   Uses `tqdm` for progress bars during data retrieval.

## Requirements

-   Python 3.x
-   pandas
-   comtradeapicall
-   tqdm
-   dateutil

You can install these packages using pip:

```

pip install pandas comtradeapicall tqdm python-dateutil

```

## Installation

1.  Clone the repository:

```

git clone https://github.com/yourusername/trade-data-analysis-tool.git
cd trade-data-analysis-tool

```

2.  Install the required packages:

```

pip install -r requirements.txt

```

## Usage

1.  **Configure Data Retrieval**:
    -   Modify `main.py` to set the desired `year`, `flow_code` (e.g., 'M' for imports), and `data_dir`.
    -   Ensure `countries.json` and `products_hs_nomenclature.csv` are correctly populated.
2.  **Run the Script**:

```

python main.py

```

3.  **Data Output**:

    -   The fetched data will be saved in the `data/raw` directory as CSV files.
    -   Filenames are formatted as `{country_name}_{product_name}_trade_data_{year}.csv`.

## File Structure

```

.
├── .gitignore                     \# Specifies intentionally untracked files that Git should ignore
├── countries.json                 \# JSON file containing list of countries with ISO3 codes and Comtrade codes
├── helper.py                      \# Contains helper functions for data fetching, structuring, and saving
├── LICENSE                        \# GNU General Public License v3.0
├── main.py                        \# Main script to execute data fetching and processing
├── products_hs_nomenclature.csv   \# CSV file mapping product descriptions to HS codes
└── README.md                      \# This file

```

## Configuration Files

-   **`countries.json`**:
    -   List of countries with associated ISO3 codes and Comtrade codes.
    -   Example:

```

[
{
"Country Name": "Afghanistan",
"ISO3": "AFG",
"Code": "004"
},
...
]

```

-   **`products_hs_nomenclature.csv`**:
    -   CSV file mapping product descriptions to HS codes.
    -   Used to specify which products to fetch trade data for.
    -   Example:

```

NomenclatureCode,Tier,ProductCode,Product Description
HS,2,0101,"Live horses, asses, mules and hinnies."
HS,3,010110,"Live horses, asses, mules and hinnies - Pure-bred breeding animals"
...

```

## Key Functions

-   **`fetch_comtrade_data(period, reporter_code, cmd_code, flow_code, output_filename, data_dir)` (helper.py)**:
    -   Fetches trade data from the UN Comtrade API.
    -   Handles retries on failure.
    -   Saves raw data to a JSON file.
-   **`fetch_data_for_all_products_for_country(periods, flow_code, data_dir, product_hs_nomenclature_file, country_name, country_code)` (helper.py)**:
    -   Fetches data for all products in `products_hs_nomenclature.csv` for a specified country.
-   **`generate_monthly_periods(year)` (helper.py)**:
    -   Generates a list of monthly periods for a given year.
-   **`main.py`**:
    -   Configures and runs the data fetching and processing pipeline.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.
