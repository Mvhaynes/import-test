import requests
import json
from pymongo import MongoClient
from config import access_token
import sys

def main(): 

    # Connect to DB    ## MAKE CONFIGURABLE and change the DB used
    conn = MongoClient()
    db = conn.biotechnique_db
    collection = db.biotechnique_collection

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
        }

    find_child_records = collection.find(
        {'child record': { '$exists' : True } }
    )

    for record in find_child_records:

        # Get record IDs
        cr_id = record['child record']
        pr_id = record['id']

        # Generate query -- Alternative: dict.keys()[x] and grab the last x items 
        fields = 'cf_next_calibration_due, cf_quality_assurance_ap' ## (Edit later) Fields to push
        query = f"select {fields} from __main__ where id eq {cr_id}" 
        data = json.dumps({
        "aql": query
        })

        # Pull child record metadata
        cr_url = f"https://import.pscace.com/gateway/v1/records/search"
        cr_response = requests.request("POST", cr_url, headers=headers, data = data).json()
        
        # Error handling
        if 'errors' in cr_response.keys():
            sys.exit(1)
        
        # Get child record's custom fields (Pulls all custom fields - can be changed to pull only specific fields)
        cr_custom = cr_response['data'][0]['attributes']
        
        # Pull parent record metadata and update response
        pr_url = f"https://import.pscace.com/gateway/v1/records/{pr_id}"
        pr_response = requests.request("GET", pr_url, headers=headers).json()

        pr_payload = pr_response['data']['attributes'].update(cr_custom)

        # Push updates to parent record
        pr_url = f"https://import.pscace.com/gateway/v1/records/{pr_id}"
        requests.request("PATCH", pr_url, headers=headers, data=pr_payload)


