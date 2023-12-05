import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from google.cloud import firestore
from datetime import datetime, timedelta

# Authenticate to Firestore with the JSON account key.
db = firestore.Client.from_service_account_json("firestore-key.json")

def get_data():
    collection_ref = db.collection("your_collection_name")
    docs = collection_ref.stream()
    data = [doc.to_dict() for doc in docs]
    return data

def create_dataframe():
    data = get_data()
    df = pd.DataFrame(data)
    
    # Convert 'time' column to datetime type
    df['time'] = pd.to_datetime(df['time'])
    
    return df


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

def average_pct_margin_by_time_period(df, time_period):
    now = datetime.now()

    if time_period == 'Last 30 mins':
        start_time = now - timedelta(minutes=30)
    elif time_period == 'Last 1 hour':
        start_time = now - timedelta(hours=1)
    elif time_period == 'Last 10 hours':
        start_time = now - timedelta(hours=10)
    elif time_period == 'Last day':
        start_time = now - timedelta(days=1)
    elif time_period == 'Last week':
        start_time = now - timedelta(weeks=1)
    elif time_period == 'Last month':
        start_time = now - timedelta(weeks=4)  # Approximating a month as 4 weeks
    else:
        st.error("Invalid time period selected.")
        return None

    # Ensure both 'time' and 'start_time' are of the same datetime type
    df['time'] = pd.to_datetime(df['time'], utc=True)
    start_time = pd.to_datetime(start_time, utc=True)

    filtered_df = df[(df['time'] >= start_time) & (df['time'] <= now)]
    avg_pct_margin = filtered_df['pct_margin'].mean()

    return avg_pct_margin

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

    sum_margin_df = sum_margin_by_30_minutes(df)
    st.subheader("Bar Plot: Sum of Margin for 30-minute Intervals")

    # Widget to select time period
    time_period = st.selectbox("Select Time Period", ['Last 30 mins', 'Last 1 hour', 'Last 10 hours', 'Last day', 'Last week', 'Last month'])

    # Calculate and display average pct_margin for the selected time period
    avg_pct_margin = average_pct_margin_by_time_period(df, time_period)
    if avg_pct_margin is not None:
        st.subheader(f"Average Percentage Margin for {time_period}: {avg_pct_margin:.2f}%")

if __name__ == "__main__":
    main()
