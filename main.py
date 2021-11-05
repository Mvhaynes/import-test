import requests
import json
import sys
import os

# from config import access_token, cloud_creds
access_token = os.environ['access_token']
cloud_creds = os.environ['cloud_creds']

# Generate auth token
iam_url = "https://iam.cloud.ibm.com/identity/token"

iam_headers= {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': 'application/json'
}

data = f"grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey={cloud_creds}"

resp = requests.post(iam_url, headers=iam_headers, data=data)
resp = resp.json()
token = resp['access_token']

# Error handling
if 'errorCode' in resp.keys():
    print(token['errorMessage'])
    sys.exit(1)

# Request headers
ace_headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
}

cloud_headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# Get list of existing databases
cloud_url = 'https://446d2990-cae4-4977-8f4a-68838e1beb23-bluemix.cloudantnosqldb.appdomain.cloud'
all_db = cloud_url + '/_all_dbs'
db_resp = requests.request("GET", all_db, headers=cloud_headers)

# Error handling
if db_resp.status_code != 200:
    print('Unable to retrieve databases. Error: ', database_list['reason'])
    sys.exit(1)
    
database_list = db_resp.json()

# Instance name
instance_name = 'import-instance-test-run'

# Check name for invalid characters
def filter_name(instance):
    
    instance = instance.lower()
    filter_char = lambda char: char.isalnum() or char == '-'
    filtered = filter(filter_char, instance)
    filtered = list(filtered)
    instance_name = ''.join(filtered)
    return instance_name

instance_name = filter_name(instance_name)

# If database exists, check for updates
if instance_name in database_list:

    # Pull records from ACE
    url = f"https://import.pscace.com/gateway/v1/records?page=308" ## CHANGE THIS BACK LATER
    response = requests.request("GET", url, headers=ace_headers).json()
    response_list = response['data']
    
    # Get next pages
    while 'next' in response['links'].keys():
        
        # Get link
        next_page = response['links']['next']['href']
        next_url = "https://import.pscace.com" + next_page

        # Request next page and append list
        response = requests.request("GET", next_url, headers=ace_headers).json()
        
        for each in response['data']:
            response_list.append(each)

# If db does not exist yet, create database and pull records from ACE
elif instance_name not in database_list:
    
    # Create database
    db_url = f'{cloud_url}/{instance_name}'
    create = requests.put(db_url, headers=cloud_headers)
    
    
    # Pull all records from ACE
    ace_url = "https://import.pscace.com/gateway/v1/records?page=308"  ## Make sure to change this back to pg1 
    response = requests.request("GET", ace_url, headers=ace_headers).json()

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
        response = requests.request("GET", next_url, headers=ace_headers).json()
        
        for each in response['data']:
            response_list.append(each)
        
# Search response
for record in response_list:
    
    record_id = record['id']

    # Check if record already exists in database (avoids duplicating)
    record_url = f'{cloud_url}/{instance_name}/_all_docs'
    data = f"""
    {{
      "key": "{record_id}"
    }}
    """
    resp = requests.post(record_url, headers=cloud_headers, data=data).json()

    # If record already exists, skip
    if 'rows' in resp.keys() and resp['rows']:
        continue

    # If record is not found in database
    else:
        
        # Pull metadata
        meta_url = f"https://import.pscace.com/gateway/v1/records/{record_id}/meta"
        meta_response = requests.request("GET", meta_url, headers=ace_headers).json()

        # Check for custom field, skip to next record if custom field does not exist
        try: 
            custom_field = meta_response['data']['attributes']['cf_next_calibration_due']

        except KeyError: 
            continue

        # Get project ID
        project_id = meta_response['data']['relationships']['project']['data']['id']

        # Search for child projects
        child_project_url = f"https://import.pscace.com/gateway/v1/projects/{project_id}/children"
        cr_response = requests.request("GET", child_project_url, headers=ace_headers).json()

        # Skips record if no child projects exist
        if not cr_response['data']:  ## CHECK if record should be created anyways without a parent?
            continue
        
        # Get child project ID 
        child_project_id = cr_response['data'][0]['id']

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
        
        post_response = requests.request("POST", records_url, headers=ace_headers, data=payload)
        res = post_response.json()

        # Error handling if field does not exist
        if 'errors' in res.keys():
            print(res, record_id)
            continue
 
        # Get new child record ID
        child_record_id = res['data']['id']

        
        # Update database with new IDs (If only searching new records, do this at beginning of loop)
        db_url = f'{cloud_url}/{instance_name}'
        
        cr_data = f"""
        {{
            "_id": "{record_id}",
            "child record": "{child_record_id}"
        }}"""

        post = requests.post(db_url, headers=cloud_headers, data=cr_data)

        if post.status_code != 201:
            print('Unable to update database')
            continue
