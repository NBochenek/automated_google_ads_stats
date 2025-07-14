import datetime
import time
import google.api_core.exceptions
from google.ads.googleads.client import GoogleAdsClient
import json
from keys import credentials

json_filepath = '/tmp/data.json' # This filepath is in both main.py and GAF.py

#Resolving bad references due to GCF import bug.
timedelta = datetime.timedelta
datetime = datetime.datetime


# Set up the Google Ads client with the custom configuration
google_ads_client = GoogleAdsClient.load_from_dict(credentials)

# Specify the customer ID of the manager account
manager_customer_id = "9136996873"
google_ads_client.login_customer_id = manager_customer_id


def get_subaccounts(manager_customer_id):

    query = """
        SELECT
          customer_client.id
        FROM customer_client
        WHERE customer_client.level = 1
        AND customer_client.status = ENABLED
        """

    search_request = google_ads_client.get_type("SearchGoogleAdsStreamRequest")

    # Set the account ID
    search_request.customer_id = manager_customer_id

    # Set the query to the search_request
    search_request.query = query

    ga_service = google_ads_client.get_service("GoogleAdsService")

    # Create the stream
    stream = ga_service.search_stream(search_request)

    failed_requests = [] # Inits list for failed requests. Will be looped later.

    for batch in stream:
        for row in batch.results:
            if row.customer_client.id == "1980549923": # Skips manager account.
                continue
            print(f"Getting metrics for ID: {row.customer_client.id}")
            try:
                get_metrics(row.customer_client.id)
            except google.ads.googleads.errors.GoogleAdsException as e:
                print(e)
            except google.api_core.exceptions.ServiceUnavailable as e:
                print(e)
                failed_requests.append(row.customer_client.id)
                print("Adding client ID to list and waiting 3 Seconds...")
                # time.sleep(3)
            except google.api_core.exceptions.InternalServerError as e:
                print(e)
                failed_requests.append(row.customer_client.id)
                print("Adding client ID to list and waiting 3 Seconds...")
                # time.sleep(3)
            except Exception as e:
                print(e)

    # Loop to get the stragglers in failed requests.
    #TODO The script will time out on GCF before this runs.
    print(f"Processing {len(failed_requests)} clients that errored initially.")
    for id in failed_requests:
        print(f"Getting metrics for ID: {id}")
        try:
            get_metrics(id)
        except google.ads.googleads.errors.GoogleAdsException as e:
            print(e)
        except google.api_core.exceptions.ServiceUnavailable as e:
            print(e)
        except google.api_core.exceptions.InternalServerError as e:
            print(e)
        except Exception as e:
            print(e)




def get_metrics(customer_id):

    retrieve_data = {}
    
    # getting start and end date for query 30 days data
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # query for 30 days ctr,clicks,spends
    query = f"""
        SELECT 
        customer.id,
        customer.descriptive_name,
        metrics.clicks,
        metrics.impressions,
        metrics.cost_micros
        FROM customer
        WHERE
        segments.date BETWEEN "{start_date_str}" AND "{end_date_str}"
    """

    search_request = google_ads_client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = str(customer_id)
    search_request.query = query
    ga_service = google_ads_client.get_service("GoogleAdsService")
    try:
        stream = ga_service.search_stream(search_request)

        for batch in stream:
            for row in batch.results:
                customer_name = row.customer.descriptive_name
                print(f"Getting metrics for {customer_name}")
                clicks_30_days = row.metrics.clicks
                impressions_30_days = row.metrics.impressions
                ctr_30_days = (clicks_30_days / impressions_30_days) * \
                    100 if impressions_30_days > 0 else 0
                ctr_30_days = ctr_30_days/100 # Converts int to decimal.
                cost_micros_30_days = row.metrics.cost_micros
                spend_30_days = cost_micros_30_days / 1000000
                retrieve_data[customer_id] = {
                    'customer_name': customer_name,
                    'clicks_30_days': clicks_30_days,
                    'ctr_30_days': ctr_30_days,
                    'spend_30_days': spend_30_days
                }
    except google.ads.googleads.errors.GoogleAdsException as e:
        print(e)


    # getting start and end date for query 7 days data
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # query for 7 days ctr
    query = f"""
        SELECT
        customer.id,
        metrics.clicks,
        metrics.conversions,
        metrics.impressions
        FROM customer
        WHERE
        segments.date BETWEEN "{start_date_str}" AND "{end_date_str}"
    """

    search_request.query = query
    try:
        stream = ga_service.search_stream(search_request)
        for batch in stream:
            for row in batch.results:
                clicks_7_days = row.metrics.clicks
                impressions_7_days = row.metrics.impressions
                ctr_7_days = (clicks_7_days / impressions_7_days) * \
                    100 if impressions_7_days > 0 else 0
                ctr_7_days = ctr_7_days/100 # Converts int to decimal.
                conversions_7_days = row.metrics.conversions
                conversion_rate_7_days = (
                                                 conversions_7_days / clicks_7_days) if clicks_7_days > 0 else 0
                retrieve_data[customer_id].update(
                    {'ctr_7_days': ctr_7_days, "conversion_rate_7_days": conversion_rate_7_days})
    except google.ads.googleads.errors.GoogleAdsException as e:
        print(e)

    # Get the start and end date for the previous 7 days (last week).
    end_date = datetime.now().date() - timedelta(days=8)
    start_date = end_date - timedelta(days=7)  # To get 7 full days excluding today
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    query = f"""
            SELECT
            customer.id,
            metrics.clicks,
            metrics.impressions,
            metrics.ctr,
            metrics.conversions_from_interactions_rate,
            metrics.conversions
            FROM customer
            WHERE
            segments.date BETWEEN "{start_date_str}" AND "{end_date_str}"
            AND customer.status = 'ENABLED' 
        """

    search_request.query = query
    try:
        stream = ga_service.search_stream(search_request)
        for batch in stream:
            for row in batch.results:
                cr_7_days = row.metrics.conversions_from_interactions_rate
                retrieve_data[customer_id].update({
                    "converstion_rate_previous_7_days": cr_7_days
                })
    except google.ads.googleads.errors.GoogleAdsException as e:
        print(e)

    # getting start and end date for query last month data
    end_date = datetime.now().date().replace(day=1) - timedelta(days=1)
    start_date = end_date.replace(day=1)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # query for last month ctr,clicks, spend
    query = f"""
        SELECT
        customer.id,
        metrics.clicks,
        metrics.impressions,
        metrics.cost_micros,
        metrics.conversions
        FROM customer
        WHERE
        segments.date BETWEEN "{start_date_str}" AND "{end_date_str}"
    """

    search_request.query = query
    try:
        stream = ga_service.search_stream(search_request)
        for batch in stream:
            for row in batch.results:
                clicks_last_month = row.metrics.clicks
                impressions_last_month = row.metrics.impressions
                ctr_last_month = (clicks_last_month / impressions_last_month) * \
                    100 if impressions_last_month > 0 else 0
                ctr_last_month = ctr_last_month/100 # Converts int to decimal.
                spend_last_month = row.metrics.cost_micros / 1000000
                conversions_last_month = row.metrics.conversions
                retrieve_data[customer_id].update(
                    {'clicks_last_month': clicks_last_month, 'ctr_last_month': ctr_last_month, 'spend_last_month': spend_last_month,
                     'impressions_last_month': impressions_last_month, 'conversions_last_month': conversions_last_month})
                # print(f"Debug Impressions: {retrieve_data[customer_id].get('impressions_last_month')}")
    except google.ads.googleads.errors.GoogleAdsException as e:
        print(e)

    # getting start and end date for query 14 days data
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=14)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    query = f"""
        SELECT
        customer.id,
        metrics.conversions,
        metrics.clicks
        FROM customer
        WHERE
        segments.date BETWEEN "{start_date_str}" AND "{end_date_str}"
    """

    search_request.query = query
    try:
        stream = ga_service.search_stream(search_request)
    except google.api_core.exceptions.ServiceUnavailable as e:
        print(e)
        print("Waiting 3 Seconds...")
        time.sleep(3)


    for batch in stream:
        for row in batch.results:
            conversions_14_days = row.metrics.conversions
            clicks_14_days = row.metrics.clicks
            conversion_rate_14_days = (
                conversions_14_days / clicks_14_days) * 100 if clicks_14_days > 0 else 0
            conversion_rate_14_days = conversion_rate_14_days/100 # Converts int to decimal.
            retrieve_data[customer_id].update(
                {'conversion_rate_14_days': conversion_rate_14_days})

    # Calculate dates for Last Month To Date query.
    # Get the current date
    current_date = datetime.now().date()
    # Calculate the first day of the previous month
    first_day_of_previous_month = current_date.replace(day=1) - timedelta(days=1)
    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)
    # Set the start date as the first day of the previous month
    start_date_lmd = first_day_of_previous_month
    # Calculate the last month to date, which should be the same day of the last month as today
    end_date_lmd = current_date.replace(day=1) - timedelta(days=1)
    end_date_lmd = end_date_lmd.replace(day=current_date.day)
    # Check if last month had fewer days than today's date, if so, use the last day of last month
    if end_date_lmd.day > current_date.day:
        end_date_lmd = current_date.replace(day=1) - timedelta(days=1)
    # Convert the start and end dates to string representations for the Last Month To Date query
    start_date_str = start_date_lmd.strftime("%Y-%m-%d")
    end_date_str = end_date_lmd.strftime("%Y-%m-%d")

    # query for last month to date clicks
    query = f"""
        SELECT
        customer.id,
        metrics.clicks,
        metrics.impressions
        FROM customer
        WHERE
        segments.date BETWEEN "{start_date_str}" AND "{end_date_str}"
    """

    search_request.query = query
    stream = ga_service.search_stream(search_request)
    for batch in stream:
        for row in batch.results:
            clicks_last_month_to_date = row.metrics.clicks
            retrieve_data[customer_id].update(
                {'clicks_last_month_to_date': clicks_last_month_to_date})

    # getting start and end date for query this month to date data
    end_date = datetime.now().date()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date_str = end_date.replace(day=1).strftime("%Y-%m-%d")

    # query for this month to date clicks,spends
    query = f"""
        SELECT 
        metrics.clicks,
        metrics.cost_micros
        FROM customer
        WHERE
        segments.date BETWEEN "{start_date_str}" AND "{end_date_str}"
    """

    search_request.query = query
    stream = ga_service.search_stream(search_request)

    for batch in stream:
        for row in batch.results:
            clicks_this_month_to_date = row.metrics.clicks
            cost_micros_this_month_to_date = row.metrics.cost_micros
            spend_this_month_to_date = cost_micros_this_month_to_date / 1000000
            retrieve_data[customer_id].update(
                {'clicks_this_month_to_date': clicks_this_month_to_date, 'spend_this_month_to_date': spend_this_month_to_date})

    #Add date data to the dict
    today = datetime.now().date().strftime("%Y-%m-%d")
    retrieve_data[customer_id].update({'date_generated': today})
    # print(retrieve_data)

    update_data(retrieve_data)
    print(f"Finished gathering data for {customer_name}.")


# function to write retrieve data into data.json
def update_data(data_to_add):
    with open(json_filepath) as data_file:
        data = json.load(data_file)

    data.append(data_to_add)
    with open(json_filepath, "w") as data_file:
        json.dump(data, data_file, indent=4)
        print(len(data)) # Prints number of accounts in JSON.


def get_ads_data():
    # Call the function to retrieve subaccounts and corresponding data.
    get_subaccounts(manager_customer_id)