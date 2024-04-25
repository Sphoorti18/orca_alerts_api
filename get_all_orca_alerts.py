import os
import argparse
import json
import pandas as pd
import hsdb_get_all_alerts as hsdb_get_all
from orca_utils import client as orca

# Read environment variables for Orca API
ORCA_API_KEY = os.environ.get('ORCA_API_KEY')
ORCA_BASE_URL = os.environ.get('ORCA_API_BASE_URL')

# Define a DSL filter to filter out alerts from a specific source
local_filter = {
    "dsl_filter": {
        "filter": [
            {
                "field": "state.rule_source",
                "includes": ["Custom"]
            }
        ]
    }
}

def get_all_alerts_paginated(dsl_filter, filename, status=["open", "in_progress"], show_informational_alerts=False, show_all_statuses_alerts=False, page_size=1000):
   
    # Local var initialization
    number_of_alerts = orca.get_alerts(dsl_filter=dsl_filter, count=True, status=status)
    alerts_so_far = 0
    missing_alert_cnt = 0
    found_alert_cnt = 0
    
    # Load local alerts into a pandas DataFrame
    try:
        df = pd.read_csv(f'{filename}.csv')
        local_alerts_set = set(df['alert_id'])
    except:
        print(f'No file with the name {filename}.csv has been found! Make sure to use --refresh first to create a local copy of the HSDB database (which may take several hours).')
        return
    
        # Clear output files
    with open("alerts_missing.csv", "w") as output_missing:
        output_missing.write('alert_id,status\n')
    with open("alerts_found.csv", "w") as output_found:
        output_found.write('alert_id,status\n')
    
    # Call the API
    payload = {
        "limit": page_size,
        **dsl_filter,
        "dsl_filter": {
            **dsl_filter.get("dsl_filter", {}),
            "filter": dsl_filter.get("dsl_filter", {}).get("filter", []) + [{"field": "state.status", "includes": status}],
        }
    }
    
    while True:
        response = orca.request("POST", orca.get_url_by_endpoint('/query/alerts'), headers=orca.get_headers(ORCA_API_KEY), data=json.dumps(payload)).json()
        if response['data']:
            for result in response['data']:
                alerts_so_far += 1
                # Compare Orca alert to local HSDB database and write to according output file
                if result["state"]["alert_id"] not in local_alerts_set:
                    with open("alerts_missing.csv", "a") as alerts_missing:
                        alerts_missing.write(f'{result["state"]["alert_id"]},{result["state"]["status"]}\n')
                        missing_alert_cnt += 1
                else:
                    with open("alerts_found.csv", "a") as alerts_found:
                        alerts_found.write(f'{result["state"]["alert_id"]},{result["state"]["status"]}\n')
                        found_alert_cnt += 1
            print(f'Page: {alerts_so_far // page_size} - Alerts left: {number_of_alerts - alerts_so_far} - Alerts processed: {alerts_so_far} - Missing alerts: {missing_alert_cnt} - Found alerts: {found_alert_cnt}')
        if 'next_page_token' in response:
            payload['next_page_token'] = response['next_page_token']
        else:
            print('All pages processed.')
            break

def main(refresh, filename):
    if refresh:
        hsdb_get_all.main(filename=filename)
    get_all_alerts_paginated(dsl_filter=local_filter, filename=filename)
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--refresh', action='store_true',
                        help='Refresh the local copy of the HSDB database. Please be aware that this may take around 6 hours!')
    parser.add_argument('--filename', default='hsdb', 
                        help='The filename for the local HSDB database file (to create or to find). The file will be saved as csv, no extension required in the filename! If not provided, default filename "hsdb" will be used.')
    args = parser.parse_args()
    main(args.refresh, args.filename)