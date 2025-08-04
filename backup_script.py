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
    """Fetches all items and column structures for a given board ID."""
    column_query = f'{{ boards(ids: {board_id}) {{ columns {{ id title }} }} }}'
    try:
        response = requests.post(MONDAY_API_URL, json={'query': column_query}, headers=HEADERS)
        response.raise_for_status()
        columns_data = response.json()['data']['boards'][0]['columns']
        column_map = {c['id']: c['title'] for c in columns_data}
    except Exception as e:
        print(f"Error fetching columns for board {board_id}: {e}")
        return None

    items_data = []
    page = 1
    while True:
        items_query = f'''
        {{
          boards(ids: {board_id}) {{
            items_page(limit: 100, page: {page}) {{
              cursor
              items {{
                id
                name
                column_values {{
                  id
                  text
                }}
              }}
            }}
          }}
        }}
        '''
        try:
            response = requests.post(MONDAY_API_URL, json={'query': items_query}, headers=HEADERS)
            response.raise_for_status()
            result = response.json()
            if "errors" in result:
                print(f"Monday API Error: {result['errors']}")
                break
            current_items = result['data']['boards'][0]['items_page']['items']
            if not current_items:
                break
            items_data.extend(current_items)
            if result['data']['boards'][0]['items_page']['cursor'] is None:
                break
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching items for board {board_id} on page {page}: {e}")
            return None

    processed_rows = []
    for item in items_data:
        row = {'Item ID': item['id'], 'Item Name': item['name']}
        for col_val in item['column_values']:
            column_title = column_map.get(col_val['id'], col_val['id'])
            row[column_title] = col_val['text']
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
            filename = f"{safe_board_name}.csv"
            df = pd.DataFrame(board_data)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"Created CSV file: {filename} with {len(df)} rows.")
            upload_to_gdrive(gdrive_service, filename, GDRIVE_FOLDER_ID)
            os.remove(filename)
        else:
            print(f"No data to process for board '{board_name}'.")

if __name__ == "__main__":
    main()
