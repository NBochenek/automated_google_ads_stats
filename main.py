import json
import os
from google.ads.googleads.client import GoogleAdsClient
from Google_Ads_Functions import get_ads_data
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from spreadsheet_parameters import column_order, spreadsheet_name, worksheet_id, spreadsheet_id
from send_email import send_email


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
    df = load_json_to_df('/tmp/data.json')

    # Write data to Google Sheets
    df_to_gsheets(df, 'Service-Account_Key.json', spreadsheet_name, column_order, worksheet_id)


def purge_json():
    # Deletes JSON to start from scratch.
    file_path = '/tmp/data.json'  # Save the file in the /tmp directory. This absolute reference only works on GCF.
    if os.path.exists(file_path):
        os.remove(file_path)
        print('File removed successfully')
    else:
        print("File not found.")
    # Creates New File
    file = open(file_path, "w")
    file.write("["
               "]")
    file.close()


def purge_get_write(request):
    try:
        # Clears JSON file of Ads Data
        purge_json()

        # # Runs main Google Ads functions to gather data to JSON.
        get_ads_data()

        # # Write the dataframe to the sheet.
        load_df_and_write_to_sheet()

        return "Function completed successsfully!"
    except:
        send_email(
            "Error in Automated Stats = Google Ads",
            "See subject. Investigate in the Google Cloud Console:"
        "https: // console.cloud.google.com / welcome?cloudshell = false & project = npm - automated - stats - ads"
        )
        return "Function failed. Email sent."


if __name__ == "__main__":
    purge_get_write("")
