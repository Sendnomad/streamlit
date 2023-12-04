import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from google.cloud import firestore

# Authenticate to Firestore with the JSON account key.
db = firestore.Client.from_service_account_json("firestore-key.json")

# Function to retrieve data from Firestore
def get_data():
    collection_ref = db.collection("your_collection_name")
    docs = collection_ref.stream()
    data = [doc.to_dict() for doc in docs]
    return data

# Function to create a dataframe from the retrieved data
def create_dataframe():
    data = get_data()
    df = pd.DataFrame(data)
    return df

# Function to visualize with Matplotlib
def visualize_with_matplotlib(df):
    st.subheader("Visualization with Matplotlib")
    fig, ax = plt.subplots()
    ax.plot(df['transaction_id'], df['crypto_received'], label='Crypto Received')
    ax.plot(df['transaction_id'], df['crypto_spent'], label='Crypto Spent')
    ax.set_xlabel('Transaction ID')
    ax.set_ylabel('Amount')
    ax.legend()
    st.pyplot(fig)

# Function to visualize with Plotly
def visualize_with_plotly(df):
    st.subheader("Visualization with Plotly")
    fig = px.line(df, x='transaction_id', y=['crypto_received', 'crypto_spent'],
                 labels={'value': 'Amount', 'variable': 'Type'},
                 title='Crypto Transactions Over Time')
    st.plotly_chart(fig)

# Streamlit App
# Streamlit App
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

if __name__ == "__main__":
    main()

