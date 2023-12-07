import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from google.cloud import firestore
from datetime import datetime, timedelta
import sqlite3
import json
from google.oauth2 import service_account
from google.protobuf.timestamp_pb2 import Timestamp

local_testing = True

if (local_testing):
    # Authenticate to Firestore with the JSON account key.
    db = firestore.Client.from_service_account_json("firebase-key.json")
else:
    # Authenticate to Firestore with the JSON account key.
    key_dict = json.loads(st.secrets["firestore"])
    creds = service_account.Credentials.from_service_account_info(key_dict)
    db = firestore.Client(credentials=creds, project="streamlit")

# SQLite database connection
sqlite_conn = sqlite3.connect('firestore.db')
sqlite_cursor = sqlite_conn.cursor()

def create_table_if_not_exists():
    # Create the table if it doesn't exist
    sqlite_cursor.execute('''
        CREATE TABLE IF NOT EXISTS accounting (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actualAmount REAL,
            amount REAL,
            blockchainTransferId TEXT,
            blockchainaddress TEXT,
            cashbackLosses REAL,
            cashedOut INTEGER,
            completed INTEGER,
            cryptocurrency TEXT,
            fromCurrency TEXT,
            hasCashRewards INTEGER,
            mercuryoFiatAmount REAL,
            mercuryoFromCurrency TEXT,
            nickname TEXT,
            orderNumber TEXT,
            p2pRateRealized REAL,
            profit REAL,
            profitActual REAL,
            recipientAmount REAL,
            recipientAmountDelivered TEXT,
            recipientCurrency TEXT,
            recipientPayoutOption TEXT,
            sendCountry TEXT,
            systemId TEXT,
            time_created DATETIME,
            username TEXT,
            valueCryptoReceived REAL,
            valueCryptoUsed REAL,
            valueCryptoUsedActual REAL
        )
    ''')
    sqlite_conn.commit()

def firestore_to_python_type(value, data_type):
    if value is not None:
        if data_type == 'string':
            # Check if it's a numeric string and convert to float if possible
            try:
                return float(value)
            except ValueError:
                # If conversion to float fails, return as string
                return str(value)
        elif data_type == 'number':
            try:
                return float(value)
            except ValueError:
                return None
        elif data_type == 'boolean':
            if value == True:
                return 1
            else :
                return 0
        # elif data_type == 'timestamp':
        #     return datetime.utcfromtimestamp(value.seconds + value.nanos / 1e9)
    return None  # Return None for empty values

def get_data_from_firestore():
    start_timestamp = datetime(2023, 12, 1)
    collection_ref = db.collection("accounting")
    docs = collection_ref.where('time_created', '>=', start_timestamp).stream()
    data = [doc.to_dict() for doc in docs]
    return data

def get_data_from_sqlite():
    sqlite_cursor.execute("SELECT * FROM accounting")
    data = sqlite_cursor.fetchall()
    return data

def update_sqlite_cache(data):
    try:
        values = [(firestore_to_python_type(entry.get('actualAmount'), 'number'),
            firestore_to_python_type(entry.get('amount'), 'number'),
            entry.get('blockchainTransferId'),
            entry.get('blockchainaddress'),
            firestore_to_python_type(entry.get('cashbackLosses'), 'number'),
            firestore_to_python_type(entry.get('cashedOut'), 'boolean'),
            firestore_to_python_type(entry.get('completed'), 'boolean'),
            entry.get('cryptocurrency'),
            entry.get('fromCurrency'),
            firestore_to_python_type(entry.get('hasCashRewards'), 'boolean'),
            firestore_to_python_type(entry.get('mercuryoFiatAmount'), 'number'),
            entry.get('mercuryoFromCurrency'),
            entry.get('nickname'),
            entry.get('orderNumber'),
            firestore_to_python_type(entry.get('p2pRateRealized'), 'number'),
            firestore_to_python_type(entry.get('profit'), 'number'),
            firestore_to_python_type(entry.get('profitActual'), 'number'),
            firestore_to_python_type(entry.get('recipientAmount'), 'number'),
            firestore_to_python_type(entry.get('recipientAmountDelivered'), 'number'),
            entry.get('recipientCurrency'),
            entry.get('recipientPayoutOption'),
            entry.get('sendCountry'),
            entry.get('systemId'),
            entry.get('time_created').strftime('%Y-%m-%d %H:%M:%S') if entry.get('time_created') else None,
            entry.get('username'),
            firestore_to_python_type(entry.get('valueCryptoReceived'), 'number'),
            firestore_to_python_type(entry.get('valueCryptoUsed'), 'number'),
            firestore_to_python_type(entry.get('valueCryptoUsedActual'), 'number'))
        for entry in data]
    except KeyError as e:
        st.error(f"KeyError: {e} is missing in data. Check your data structure.")
        return

    sqlite_cursor.executemany('''
        INSERT OR IGNORE INTO accounting 
        (actualAmount, amount, blockchainTransferId, blockchainaddress, cashbackLosses,
         cashedOut, completed, cryptocurrency, fromCurrency, hasCashRewards, mercuryoFiatAmount,
         mercuryoFromCurrency, nickname, orderNumber, p2pRateRealized, profit, profitActual,
         recipientAmount, recipientAmountDelivered, recipientCurrency, recipientPayoutOption,
         sendCountry, systemId, time_created, username, valueCryptoReceived, valueCryptoUsed,
         valueCryptoUsedActual)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', values)
    sqlite_conn.commit()


def create_dataframe():
    create_table_if_not_exists()
    
    # Fetch data from SQLite
    sqlite_data = get_data_from_sqlite()

    # If SQLite has data, fetch only missing data from Firestore
    if sqlite_data:
        firestore_data = get_missing_data_from_firestore(sqlite_data)
    else:
        # If SQLite is empty, fetch all data from Firestore
        firestore_data = get_data_from_firestore()

    # Update SQLite cache with new data
    update_sqlite_cache(firestore_data)

    # Fetch data from SQLite again
    fresh_sqlite_data = get_data_from_sqlite()

    df = pd.DataFrame(fresh_sqlite_data)
    
    return df

def get_missing_data_from_firestore(sqlite_data):
    # Get the latest time from SQLite
    latest_time = max(entry[0] for entry in sqlite_data)

    # Fetch data from Firestore that is not in SQLite
    collection_ref = db.collection("accounting")
    docs = collection_ref.where('time_created', '>', latest_time).stream()
    data = [doc.to_dict() for doc in docs]
    
    return data

def main():
    st.title("Crypto Transactions Visualization")

    # Retrieve data from Firestore and create a dataframe
    df = create_dataframe()

if __name__ == "__main__":
    main()
