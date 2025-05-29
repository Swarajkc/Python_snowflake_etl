import requests
import pandas as pd
import os
import time
from dotenv import load_dotenv

# Load credentials
load_dotenv()
client_id = os.getenv("DOMO_CLIENT_ID")
client_secret = os.getenv("DOMO_CLIENT_SECRET")

# --- Step 1: Get Access Token ---
def get_access_token():
    auth_url = "https://api.domo.com/oauth/token"
    response = requests.post(
        auth_url,
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials"}
    )

    if response.status_code == 200:
        print("Access token acquired.")
        return response.json()["access_token"]
    else:
        print("Failed to authenticate:", response.status_code)
        print(response.text)
        return None

# --- Step 2: List All Datasets ---
def list_datasets(headers):
    response = requests.get("https://api.domo.com/v1/datasets", headers=headers)
    if response.status_code == 200:
        datasets = response.json()
        print(f"\n📊 Total Datasets: {len(datasets)}\n")
        for ds in datasets:
            print(f"🔹 Name    : {ds['name']}")
            print(f"🔹 ID      : {ds['id']}")
            print(f"🔹 Rows    : {ds['rows']} | Columns: {ds['columns']}")
            print(f"🔹 Created : {ds['createdAt']}")
            print(f"🔹 Updated : {ds['updatedAt']}")
            print("-" * 50)

        return datasets
    else:
        print("Failed to list datasets:", response.status_code)
        return []

# --- Step 3: Download a Dataset ---
def download_dataset(headers, dataset_id, save_path="downloaded.csv"):
    url = f"https://api.domo.com/v1/datasets/{dataset_id}/data"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(save_path, "wb") as f:
            f.write(response.content)
        print(f"Dataset downloaded and saved to {save_path}")
    else:
        print("Failed to download dataset:", response.status_code)

# --- Step 4: Upload CSV to Existing Dataset ---
def upload_to_dataset(headers, dataset_id, df):
    csv_data = df.to_csv(index=False)
    url = f"https://api.domo.com/v1/datasets/{dataset_id}/data"
    response = requests.put(url, headers={**headers, "Content-Type": "text/csv"}, data=csv_data)

    if response.status_code in [200, 204]:
        print("Upload successful. (Status code:", response.status_code, ")")
    else:
        print("Upload failed:", response.status_code, response.text)


# --- Step 5: Create New Dataset ---
def create_dataset(headers):
    payload = {
        "name": "Demo Python Dataset",
        "description": "Created from Python using Domo Platform API",
        "schema": {
            "columns": [
                {"type": "STRING", "name": "Company"},
                {"type": "LONG", "name": "Price"},
                {"type": "LONG", "name": "Volume"},
                {"type": "DATE", "name": "Date"}
            ]
        }
    }

    url = "https://api.domo.com/v1/datasets"
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 201:
        dataset_id = response.json()["id"]
        print("Dataset created. ID:", dataset_id)
        return dataset_id
    else:
        print("Failed to create dataset:", response.status_code)
        print(response.text)
        return None

# --- Main Execution ---
if __name__ == "__main__":
    access_token = get_access_token()
    if not access_token:
        exit()

    headers = {"Authorization": f"Bearer {access_token}"}

    # 1. List all datasets
    datasets = list_datasets(headers)

    # 2. Download the first dataset (if available)
    if datasets:
        first_id = datasets[0]['id']
        download_dataset(headers, first_id)

    # 3. Create a new dataset
    new_id = create_dataset(headers)

    # 4. Upload a DataFrame to the new dataset
    if new_id:
        time.sleep(3)
        df = pd.DataFrame({
            "Company": ["NABIL", "NLIC"],
            "Price": [820, 1205],
            "Volume": [1000, 500],
            "Date": ["2025-05-29", "2025-05-29"]
        })
        print("Uploading this DataFrame:")
        print(df)
        print("Columns:", df.columns.tolist())
        print("Dtypes:\n", df.dtypes)

        time.sleep(3)
        upload_to_dataset(headers, new_id, df)
