import streamlit as st
from google.cloud import firestore
import random
from datetime import datetime

# Authenticate to Firestore with the JSON account key.
db = firestore.Client.from_service_account_json("firestore-key.json")

# Function to generate random data
def generate_random_data():
    transaction_id = st.session_state.current_transaction_id
    st.session_state.current_transaction_id += 1
    
    crypto_received = random.uniform(30, 3000)
    pct_margin = random.uniform(0, 5)
    margin = (crypto_received * pct_margin) / 100
    crypto_spent = crypto_received - margin

    return {
        'transaction_id': transaction_id,
        'crypto_received': crypto_received,
        'crypto_spent': crypto_spent,
        'margin': margin,
        'pct_margin': pct_margin,
        'time': datetime.now(),
    }

# Create a reference to the Firestore collection
collection_ref = db.collection("your_collection_name")

# Initialize session state to keep track of transaction_id
if 'current_transaction_id' not in st.session_state:
    st.session_state.current_transaction_id = 0

# Get the number of random records to insert
num_records = st.slider("Select the number of records to insert:", 1, 100, 10)

# Insert random data into Firestore
for _ in range(num_records):
    data = generate_random_data()
    collection_ref.add(data)

# Display a success message
st.success(f'{num_records} random records inserted into Firestore.')


# Display a success message
st.success(f'{num_records} random records inserted into Firestore.')

