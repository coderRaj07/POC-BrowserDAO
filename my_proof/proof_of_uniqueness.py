import os
import redis
import requests
import gnupg
import zipfile
import io
import pandas as pd
import json
import logging
from deepdiff import DeepDiff  # Ensure deepdiff is installed

# Initialize Redis connection
def get_redis_client():
    try:
        redis_client = redis.StrictRedis(
            host=os.environ.get('REDIS_HOST', 'localhost'),
            port=int(os.environ.get('REDIS_PORT', 6379)),
            db=0,
            password=os.environ.get('REDIS_PWD', None),
            decode_responses=True,
            socket_timeout=30,
            retry_on_timeout=True
        )
        redis_client.ping()
        return redis_client
    except redis.ConnectionError:
        logging.warning("Redis connection failed. Proceeding without caching.")
        return None

# TODO: Remove comments later
# Fetch file mappings from API
def get_file_mappings(wallet_address):
    # validator_base_api_url = os.environ.get('VALIDATOR_BASE_API_URL')
    # endpoint = "/api/userinfo"
    # url = f"{validator_base_api_url.rstrip('/')}{endpoint}"
    # payload = {"walletAddress": wallet_address}
    # headers = {"Content-Type": "application/json"}
    # response = requests.post(url, json=payload, headers=headers)
    # if response.status_code == 200:
    #     return response.json()
    # else:
    #     logging.error(f"Failed to fetch file mappings: {response.status_code}")
        return []

# Download and decrypt file
def download_and_decrypt(file_url, gpg_signature):
    response = requests.get(file_url)
    if response.status_code == 200:
        gpg = gnupg.GPG()
        decrypted_data = gpg.decrypt(response.content, passphrase=gpg_signature)
        if decrypted_data.ok:
            return decrypted_data.data
        else:
            logging.error("Decryption failed.")
            return None
    else:
        logging.error(f"Failed to download file: {response.status_code}")
        return None

# Extract files from ZIP data
def extract_files_from_zip(zip_data):
    csv_data_frames = []
    json_data_list = []
    with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            with zip_ref.open(file_name) as file:
                if file_name.endswith('.csv'):
                    df = pd.read_csv(file)
                    csv_data_frames.append(df)
                elif file_name.endswith('.json'):
                    json_data = json.load(file)
                    json_data_list.append(json_data)
    combined_csv_data = pd.concat(csv_data_frames, ignore_index=True) if csv_data_frames else pd.DataFrame()
    return combined_csv_data, json_data_list

# Main processing function
def process_files(curr_file_id, input_dir, wallet_address):
    gpg_signature = os.environ.get("SIGN")
    redis_client = get_redis_client()
    combined_csv_data = pd.DataFrame()
    combined_json_data = []
    
    logging.info(f"Processing files for wallet address {wallet_address}")
    # Retrieve file mappings from API
    file_mappings = get_file_mappings(wallet_address)

    if redis_client:
        # Check Redis for cached data
        for file_info in file_mappings:
            file_id = file_info.get("fileId")
            if redis_client.exists(file_id):
                stored_csv_data = redis_client.hget(file_id, "csv_data")
                stored_json_data = redis_client.hget(file_id, "json_data")
                if stored_csv_data:
                    df = pd.read_json(stored_csv_data)
                    combined_csv_data = pd.concat([combined_csv_data, df], ignore_index=True)
                if stored_json_data:
                    json_data = json.loads(stored_json_data)
                    combined_json_data.extend(json_data)
    else:
        # Download, decrypt, and extract files
        for file_info in file_mappings:
            file_url = file_info.get("fileUrl")
            decrypted_data = download_and_decrypt(file_url, gpg_signature)
            if decrypted_data:
                df, json_data_list = extract_files_from_zip(decrypted_data)
                combined_csv_data = pd.concat([combined_csv_data, df], ignore_index=True)
                combined_json_data.extend(json_data_list)

    # Process current input directory CSVs
    curr_file_csv_data = pd.DataFrame()
    local_csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    for csv_file in local_csv_files:
        file_path = os.path.join(input_dir, csv_file)
        df = pd.read_csv(file_path)
        curr_file_csv_data = pd.concat([curr_file_csv_data, df], ignore_index=True)

    # Process current input directory JSONs
    curr_file_json_data = []
    local_json_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
    for json_file in local_json_files:
        file_path = os.path.join(input_dir, json_file)
        with open(file_path, 'r') as file:
            json_data = json.load(file)
            curr_file_json_data.append(json_data)

    # Identify unique rows in current CSVs
    if not combined_csv_data.empty:
        unique_curr_csv_data = pd.concat([curr_file_csv_data, combined_csv_data, combined_csv_data]).drop_duplicates(keep=False)
    else:
        unique_curr_csv_data = curr_file_csv_data

    # Identify unique JSON entries
    unique_curr_json_data = []
    for curr_json in curr_file_json_data:
        is_unique = True
        for combined_json in combined_json_data:
            if not DeepDiff(curr_json, combined_json, ignore_order=True):
                is_unique = False
                break
        if is_unique:
            unique_curr_json_data.append(curr_json)

    # Cache current file data in Redis
    if redis_client:
        redis_client.hset(curr_file_id, mapping={
            "csv_data": curr_file_csv_data.to_json(),
            "json_data": json.dumps(curr_file_json_data),
            "history_data": json.dumps(curr_file_json_data)  # Mapping current JSON data to history_data
        })
    
    logging.info(f"Current file data stored in Redis under key {curr_file_id}")
    logging.info(f"Unique CSV data: {unique_curr_csv_data}, Current CSV data: {curr_file_csv_data}")
    logging.info(f"Unique JSON data: {unique_curr_json_data}, Current JSON data: {curr_file_json_data}")
    # Return unique CSV and JSON data
    return {
        "unique_csv_data": unique_curr_csv_data,
        "unique_json_data": unique_curr_json_data
    }
