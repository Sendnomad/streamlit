import streamlit as st
from google.cloud import firestore
import random
from datetime import datetime

# Authenticate to Firestore with the JSON account key.
db = firestore.Client.from_service_account_json("firestore-key.json")

# Function to generate random data
def generate_random_data():
    return {
        'transactions': random.randint(1, 100),
        'crypto_received': random.uniform(0.1, 10.0),
        'crypto_spent': random.uniform(0.1, 10.0),
        'margin': random.uniform(0.1, 5.0),
        'pct_margin': random.uniform(1.0, 10.0),
        'time': datetime.now(),
    }

# Create a reference to the Firestore collection
collection_ref = db.collection("your_collection_name")

# Get the number of random records to insert
num_records = st.slider("Select the number of records to insert:", 1, 100, 10)

# Insert random data into Firestore
for _ in range(num_records):
    data = generate_random_data()
    collection_ref.add(data)

# Display a success message
st.success(f'{num_records} random records inserted into Firestore.')

