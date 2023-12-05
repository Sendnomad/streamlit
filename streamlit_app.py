import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from google.cloud import firestore
from datetime import datetime, timedelta
import sqlite3

# Authenticate to Firestore with the JSON account key.
db = firestore.Client.from_service_account_json("firestore-key.json")

# SQLite database connection
sqlite_conn = sqlite3.connect('firestore_cache.db')
sqlite_cursor = sqlite_conn.cursor()

def create_table_if_not_exists():
    # Create the table if it doesn't exist
    sqlite_cursor.execute('''
        CREATE TABLE IF NOT EXISTS firestore_cache (
            time TEXT PRIMARY KEY,
            crypto_received REAL,
            crypto_spent REAL,
            margin REAL,
            pct_margin REAL,
            transaction_id TEXT
        )
    ''')
    sqlite_conn.commit()

def get_data_from_firestore():
    collection_ref = db.collection("your_collection_name")
    docs = collection_ref.stream()
    data = [doc.to_dict() for doc in docs]
    return data

def get_data_from_sqlite():
    sqlite_cursor.execute("SELECT * FROM firestore_cache")
    data = sqlite_cursor.fetchall()
    return data

def update_sqlite_cache(data):
    try:
        values = [
            (entry['time'], entry['crypto_received'], entry['crypto_spent'], entry['margin'], entry['pct_margin'], entry['transaction_id'])
            for entry in data
        ]
    except KeyError as e:
        st.error(f"KeyError: {e} is missing in data. Check your data structure.")
        return

    sqlite_cursor.executemany('''
        INSERT OR IGNORE INTO firestore_cache (time, crypto_received, crypto_spent, margin, pct_margin, transaction_id)
        VALUES (?, ?, ?, ?, ?, ?)
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
    update_sqlite_cache(firestore_data + sqlite_data)

    df = pd.DataFrame(firestore_data + sqlite_data)
    
    # Convert 'time' column to datetime type
    df['time'] = pd.to_datetime(df['time'])
    
    return df

def get_missing_data_from_firestore(sqlite_data):
    # Get the latest time from SQLite
    latest_time = max(entry[0] for entry in sqlite_data)

    # Fetch data from Firestore that is not in SQLite
    collection_ref = db.collection("your_collection_name")
    docs = collection_ref.where('time', '>', latest_time).stream()
    data = [doc.to_dict() for doc in docs]
    
    return data

def visualize_with_matplotlib(df):
    st.subheader("Scatter Plot: Transaction Amount vs. Percentage Margin")
    fig, ax = plt.subplots()
    ax.scatter(df['crypto_received'], df['pct_margin'])
    ax.set_xlabel('Transaction Amount (Crypto Received)')
    ax.set_ylabel('Percentage Margin')
    st.pyplot(fig)

def sum_margin_by_30_minutes(df):
    df['time_rounded'] = df['time'].dt.floor('30min')
    sum_margin_df = df.groupby('time_rounded')['margin'].sum().reset_index()
    return sum_margin_df

def visualize_with_plotly(df):
    st.subheader("Scatter Plot: Transaction Amount vs. Percentage Margin")
    fig = px.scatter(df, x='crypto_received', y='pct_margin',
                     labels={'crypto_received': 'Transaction Amount (Crypto Received)', 'pct_margin': 'Percentage Margin'},
                     title='Transaction Amount vs. Percentage Margin')
    st.plotly_chart(fig)


def main():
    st.title("Crypto Transactions Visualization")

    # Retrieve data from Firestore and create a dataframe
    df = create_dataframe()

    # Display the last 50 transactions as a table
    st.subheader("Last 50 Transactions Table")
    st.table(df.tail(50))

    # Visualize with Matplotlib
    visualize_with_matplotlib(df)

    # Visualize with Plotly
    visualize_with_plotly(df)

    # Widget to select time period
    time_period = st.selectbox("Select Time Period", ['Last 30 mins', 'Last 1 hour', 'Last 10 hours', 'Last day', 'Last week', 'Last month'])

if __name__ == "__main__":
    main()
