import os
import redis
import requests
import gnupg
import zipfile
import io
import pandas as pd

# Initialize Redis connection
def get_redis_client():
    try:
        redis_client = redis.StrictRedis(
            host= os.environ.get('REDIS_HOST', None),
            port= os.environ.get('REDIS_PORT', 0),
            db=0,
            password= os.environ.get('REDIS_PWD', ""),
            decode_responses=True,
            socket_timeout=30,
            retry_on_timeout=True
        )

        redis_client.ping()
        return redis_client
    except redis.ConnectionError:
        return None
    
redis_client = get_redis_client()

def get_file_mappings(wallet_address):
    """
    Function that takes a walletAddress and returns an array of objects having fileId and fileUrl.
    """
    validator_base_api_url = os.environ.get('VALIDATOR_BASE_API_URL')
    endpoint = "/api/userinfo"
    url = f"{validator_base_api_url.rstrip('/')}{endpoint}"

    payload = {"walletAddress": wallet_address}  # Send walletAddress in the body
    headers = {"Content-Type": "application/json"}  # Set headers for JSON request

    response = requests.post(url, json=payload, headers=headers)  # Make POST request

    if response.status_code == 200:
        return response.json()  # Return JSON response
    else:
        return []  # Return empty list in case of an error

def download_and_decrypt(file_url, gpg_signature):
    """Downloads and decrypts a file from the given URL using GPG"""
    response = requests.get(file_url)
    if response.status_code == 200:
        gpg = gnupg.GPG()
        decrypted_data = gpg.decrypt(response.content, passphrase=gpg_signature)
        return decrypted_data.data if decrypted_data.ok else None
    return None

def extract_csvs_from_zip(zip_data):
    """Extracts CSV files from a ZIP archive and returns a combined DataFrame"""
    data_frames = []
    with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            if file_name.endswith('.csv'):
                with zip_ref.open(file_name) as file:
                    df = pd.read_csv(file)
                    data_frames.append(df)
    return pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()

def process_zip_file(curr_file_id, input_dir, wallet_address):
    """Main function to process the ZIP file and store data in Redis"""
    gpg_signature = os.environ.get("sign")
    combined_csv_data = pd.DataFrame()
    
    # Check Redis availability
    redis_available = redis_client.ping()
    
    if redis_available:
        # Retrieve file mappings from API
        file_mappings = get_file_mappings(wallet_address)
        
        # Check Redis for file_ids and retrieve csv_data
        for file_info in file_mappings:
            file_id = file_info.get("fileId")
            if redis_client.exists(file_id):
                stored_data = redis_client.hget(file_id, "csv_data")
                if stored_data:
                    df = pd.read_json(stored_data)
                    combined_csv_data = pd.concat([combined_csv_data, df], ignore_index=True)
    else:
        # Download, decrypt, and extract CSVs
        file_mappings = get_file_mappings(wallet_address)
        for file_info in file_mappings:
            file_url = file_info.get("fileUrl")
            decrypted_data = download_and_decrypt(file_url, gpg_signature)
            if decrypted_data:
                df = extract_csvs_from_zip(decrypted_data)
                combined_csv_data = pd.concat([combined_csv_data, df], ignore_index=True)
    
    # Process current input folder CSVs
    curr_file_csv_data = pd.DataFrame()
    local_csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    for csv_file in local_csv_files:
        file_path = os.path.join(input_dir, csv_file)
        df = pd.read_csv(file_path)
        curr_file_csv_data = pd.concat([curr_file_csv_data, df], ignore_index=True)
        combined_csv_data = pd.concat([combined_csv_data, df], ignore_index=True)
    
    # Deduplicate combined CSV data
    final_csv_data = combined_csv_data.drop_duplicates()
    
    # Store only current file CSV data in Redis under curr_file_id
    redis_client.hset(curr_file_id, mapping={
        "csv_data": curr_file_csv_data.to_json(),
        "history_data": "",  # Placeholder for future processing
        "html_data": ""  # Placeholder for future processing
    })
    print(f"Current file CSV data stored in Redis under key {curr_file_id}")
    
    return final_csv_data