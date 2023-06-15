import json
import os
from google.ads.googleads.client import GoogleAdsClient
from Google_Ads_Functions import get_ads_data
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from spreadsheet_parameters import column_order, spreadsheet_name, worksheet_id, spreadsheet_id



# Load JSON data into a dataframe
def load_json_to_df(file_path):
    with open(file_path) as json_file:
        data_list = json.load(json_file)

    # Flatten the dictionaries and append to a list
    flattened_data = []
    for item in data_list:
        for key, value in item.items():
            value['id'] = key
            flattened_data.append(value)

    return pd.DataFrame(flattened_data)


# Write dataframe to a Google Sheets spreadsheet
def df_to_gsheets(df, json_key_file_path, spreadsheet_name, column_order, worksheet_id):

    # Reorder the dataframe columns
    df = df.reindex(columns=column_order)

    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

    # Add your service account file
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_file_path, scope)
    client = gspread.authorize(creds)

    # Open the Google Spreadsheet (Make sure you have shared it with your service account)
    sheet = client.open_by_key(spreadsheet_id).get_worksheet_by_id(worksheet_id)

    # Write the dataframe to the Google Sheet
    set_with_dataframe(sheet, df, col=11)


# Main function
def load_df_and_write_to_sheet():
    # Load data
    df = load_json_to_df('data.json')

    # Write data to Google Sheets
    df_to_gsheets(df, 'Service-Account_Key.json', spreadsheet_name, column_order, worksheet_id)

def purge_json():
    # Removes JSON file in order to start from scratch.
    if os.path.isfile("data.json"):
        os.remove("data.json")
        print("File deleted successfully.")
        file = open("data.json", "w")
        file.write("["
                   "]")
        file.close()

    else:
        print("File not found.")


if __name__ == "__main__":
    # Clears JSON file of Ads Data
    purge_json()

    # # Runs main Google Ads functions to gather data to JSON.
    get_ads_data()

    # # Write the dataframe to the sheet.
    load_df_and_write_to_sheet()
