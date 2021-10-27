import requests
import json
from pymongo import MongoClient
import sys
import datetime
from config import access_token
import update

# Access
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
    }

# Connect to MongoDB    ## MAKE CONFIGURABLE
conn = MongoClient()
db = conn.biotechnique_db
collection = db.biotechnique_collection

# Check if database exists (Change this later)
instance = 'biotechnique_db'  ## MAKE CONFIGURABLE
dbnames = conn.list_database_names()

# Initial data pull
if instance not in dbnames:

    # Pull all records
    url = "https://import.pscace.com/gateway/v1/records?page=308"  ## Make sure to change this back to pg1 
    response = requests.request("GET", url, headers=headers).json()

    # Error handling
    if 'data' not in response.keys():
        print("Request error", response)
        sys.exit(1)

    response_list = response['data']

    # Get all pages 
    while 'next' in response['links'].keys():

        # Get link
        next_page = response['links']['next']['href']
        next_url = "https://import.pscace.com" + next_page

        # Request next page and append list
        response = requests.request("GET", next_url, headers=headers).json()
        
        for each in response['data']:
            response_list.append(each)
        
    # Get last page and add to database (date is included for logging purposes)
    last = response['meta']['currentPage']
    collection.insert_one({ 
        'Last Page Checked': last,
        'Date': datetime.datetime.now()
    })
          
elif instance in dbnames:
    
    # Get last page checked 
    last_check = collection.find_one({ 'Last Page Checked': { '$exists': True } })
    last_check = last_check['Last Page Checked']
    
    # Pull new records
    url = f"https://import.pscace.com/gateway/v1/records?page={last_check}"
    response = requests.request("GET", url, headers=headers).json()
    
    response_list = response['data']
    
    # Get next pages
    while 'next' in response['links'].keys():
        
        # Get link
        next_page = response['links']['next']['href']
        next_url = "https://import.pscace.com" + next_page

        # Request next page and append list
        response = requests.request("GET", next_url, headers=headers).json()
        
        for each in response['data']:
            response_list.append(each)
        
    # Update last page checked
    last_page = response['meta']['currentPage']
    collection.replace_one(
        { 
            'Last Page Checked': { '$exists': True } 
        },
        { 
            'Last Page Checked': last_page,
            'Date': datetime.datetime.now()
        },
        upsert = True
    )

    # Search response
for record in response_list:
    
    record_id = record['id']

    # Check if record already exists in database (avoids duplicating)
    find_record = collection.find_one({'id': {'$eq' : record_id}})
    
    # If record is not found in database
    if find_record is None:
        
        # Update database with new IDs
        collection.insert_one({ 'id': record_id })
        
        # Pull metadata
        meta_url = f"https://import.pscace.com/gateway/v1/records/{record_id}/meta"
        meta_response = requests.request("GET", meta_url, headers=headers).json()

        # Check for custom field, skip to next record if custom field does not exist
        try: 
            custom_field = meta_response['data']['attributes']['cf_next_calibration_due']

        except KeyError: 
            continue

        # Get project ID
        project_id = meta_response['data']['relationships']['project']['data']['id']

        # Search for child projects
        child_project_url = f"https://import.pscace.com/gateway/v1/projects/{project_id}/children"
        cr_response = requests.request("GET", child_project_url, headers=headers).json()

        # Skips record if no child projects exist
        if not cr_response['data']:  ## CHECK if record should be created anyways without a parent?
            print(f'No child project associated with project ID {project_id}')
            continue
        
        # Get child project ID 
        child_project_id = response['data'][0]['id']

        # Generate new child record 
        records_url = "https://import.pscace.com/gateway/v1/records"

        payload = json.dumps({
          "data": {
            "type": "records",
            "attributes": {
              "title": f"{record_id} Child Record",  # Edit naming conventions later - MAKE CONFIGURABLE
              "project_id": child_project_id,
              "status_id": 2,
              "parent_id": record_id,
              "type": 575  # Needs to change based on instance - MAKE CONFIGURABLE
            }
          }
        })
        
        post_response = requests.request("POST", records_url, headers=headers, data=payload)
        res = post_response.json()

        # Error handling if field does not exist
        if 'errors' in res.keys():
            print(res)
            sys.exit(1)
 
        # Get new child record ID
        child_record_id = res['data']['id']

        # Add child record to original collection 
        collection.update_one(
            { 'id': record_id },
            { '$set': { 'child record': child_record_id } }
        )

# Call update script
update.main()

