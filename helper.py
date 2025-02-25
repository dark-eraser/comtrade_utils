import json
import pandas as pd
import comtradeapicall
from datetime import date, timedelta
import logging

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
    include_desc=True
):
    """
    Fetches trade data from the UN Comtrade API and saves it directly to a CSV file.
    """
    try:
        # Create the data directory if it doesn't exist
        import os
        os.makedirs(data_dir, exist_ok=True)

        # Construct the full file path
        full_filename = os.path.join(data_dir, output_filename)
        if not full_filename.endswith(".csv"):
            full_filename += ".csv"  # add the extension if it is missing

        # Log the API call parameters
        logger.info(f"Fetching data with parameters: type_code={type_code}, freq_code={freq_code}, "
                    f"cl_code={cl_code}, period={period}, reporter_code={reporter_code}, "
                    f"cmd_code={cmd_code}, flow_code={flow_code}")

        # Fetch data using comtradeapicall
        mydf = comtradeapicall.previewFinalData(
            typeCode=type_code, freqCode=freq_code, clCode=cl_code, period=period,
            reporterCode=reporter_code, cmdCode=cmd_code, flowCode=flow_code,
            partnerCode=partner_code, partner2Code=partner2_code, customsCode=customs_code,
            motCode=mot_code, maxRecords=max_records, format_output=format_output,
            aggregateBy=aggregate_by, breakdownMode=breakdown_mode, countOnly=count_only,
            includeDesc=include_desc
        )

        if isinstance(mydf, pd.DataFrame):
            if not mydf.empty:
                # Select and reorder columns
                columns = [
                    "refYear", "refMonth", "reporterDesc", "partnerDesc", "flowDesc",
                    "cmdDesc", "cifvalue"
                ]
                mydf = mydf[columns]

                # Format large numbers with commas
                number_cols = ["cifvalue", "fobvalue", "primaryValue", "qty", "netWgt", "grossWgt"]
                for col in number_cols:
                    if col in mydf.columns:
                        mydf[col] = mydf[col].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "")

                # Save directly to CSV
                mydf.to_csv(full_filename, index=False)
                logger.info(f"Data fetched and saved to: {full_filename}")
                return full_filename
            else:
                logger.warning(f"Empty DataFrame returned for reporter_code: {reporter_code}")
        else:
            logger.error(f"Unexpected return type from API: {type(mydf)}")
            if isinstance(mydf, dict):
                logger.error(f"API response: {json.dumps(mydf, indent=2)}")
            else:
                logger.error(f"API response: {mydf}")

        return None

    except Exception as e:
        logger.exception(f"Error fetching or saving data: {e}")
        return None

# Example Usage:
# # 1. Fetch the data
# json_file_path = fetch_   comtrade_data(
#     period='202205', # You can change the period
#     output_filename='tmp.json',
#     data_dir='data/raw' #change directorty
# )

# # 2. Structure the data if the fetching was successful
# if json_file_path:
#     df = structure_trade_data(json_file_path)

#     if df is not None:
#         print(df.to_string()) # Print the DataFrame to the console
#         df.to_csv('data/trade_data.csv', index=False) # Save to CSV
#         print("Data saved to trade_data.csv")

# if __name__ == "__main__" :
#     fetch_comtrade_data()