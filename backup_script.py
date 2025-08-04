import os
import requests
import pandas as pd
import json
import re
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- CONFIGURATION ---
MONDAY_API_KEY = os.getenv('MONDAY_API_KEY')
GDRIVE_FOLDER_ID = os.getenv('GDRIVE_FOLDER_ID')

MONDAY_API_URL = "https://api.monday.com/v2"
HEADERS = {"Authorization": MONDAY_API_KEY}

def get_all_boards():
    """Fetches all boards from monday.com."""
    query = '{ boards(limit: 500) { id name } }'
    try:
        response = requests.post(MONDAY_API_URL, json={'query': query}, headers=HEADERS)
        response.raise_for_status()
        boards = response.json()['data']['boards']
        print(f"Found {len(boards)} boards.")
        return boards
    except requests.exceptions.RequestException as e:
        print(f"Error fetching boards: {e}")
        return []

def get_board_data(board_id):
    """DIAGNOSTIC VERSION: Fetches only item ID and name for a given board ID."""
    
    # --- THIS IS THE DIAGNOSTIC PART ---
    # This query is the simplest possible request for items.
    # It does NOT ask for any column_values.
    items_query = f'''
    {{
      boards(ids: {board_id}) {{
        items(limit: 5000) {{
          id
          name
        }}
      }}
    }}
    '''
    try:
        print("Attempting simplified item query...")
        response = requests.post(MONDAY_API_URL, json={'query': items_query}, headers=HEADERS)
        response.raise_for_status()
        result = response.json()
        if "errors" in result:
            print(f"Monday API Error: {result['errors']}")
            return None
        items_data = result['data']['boards'][0]['items']
        print(f"Successfully fetched {len(items_data)} items (ID and Name only).")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching items for board {board_id}: {e}")
        return None

    # Process items into a list of dictionaries for the CSV
    processed_rows = []
    for item in items_data:
        # We are only processing the name and ID.
        row = {'Item ID': item['id'], 'Item Name': item['name']}
        processed_rows.append(row)
        
    return processed_rows

def upload_to_gdrive(service, file_path, folder_id):
    """Uploads a file to a specific Google Drive folder."""
    file_metadata = {
        'name': os.path.basename(file_path),
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, mimetype='text/csv')
    try:
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"Successfully uploaded {os.path.basename(file_path)} to Google Drive. File ID: {file.get('id')}")
    except Exception as e:
        print(f"Error uploading {os.path.basename(file_path)} to Google Drive: {e}")

def main():
    """Main function to run the backup process."""
    if not all([MONDAY_API_KEY, GDRIVE_FOLDER_ID]):
        print("Error: Missing one or more required environment variables.")
        return

    try:
        credentials, project = google.auth.default(scopes=['https://www.googleapis.com/auth/drive'])
        gdrive_service = build('drive', 'v3', credentials=credentials)
    except Exception as e:
        print(f"Error authenticating with Google Cloud: {e}")
        return
        
    boards = get_all_boards()
    if not boards:
        print("No boards found or error fetching boards. Exiting.")
        return

    for board in boards:
        board_id = board['id']
        board_name = board['name']
        print(f"\n--- Processing Board: {board_name} (ID: {board_id}) ---")
        board_data = get_board_data(board_id)
        if board_data:
            safe_board_name = re.sub(r'[\\/*?:"<>|]', "", board_name)
            filename = f"{safe_board_name} (Basic).csv"
            df = pd.DataFrame(board_data)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"Created CSV file: {filename} with {len(df)} rows.")
            upload_to_gdrive(gdrive_service, filename, GDRIVE_FOLDER_ID)
            os.remove(filename)
        else:
            print(f"No data to process for board '{board_name}'.")

if __name__ == "__main__":
    main()
