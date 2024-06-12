import chromadb
import os
from dotenv import load_dotenv

import pandas as pd 
import streamlit as st

#Fixed names from chromadb
CHROMA_ID = "ids"
CHROMA_DOCS = "documents"
CHROMA_METADATA = "metadatas"

def setup_client(dir):
    return chromadb.PersistentClient(path=dir)

def collection_names(client):
    print(f"collections list: {client.list_collections()}")
    return sorted([collection.name for collection in client.list_collections()])

def setup_selection(available_connections):

    return st.selectbox(
        'Selecione o Contexto',
        (available_connections)
    )

def open_metadata(metadata):

    metadata_keys = list(metadata[0].keys()) # Get keys from the first metadata dict

    out_dict = {}

    for key in metadata_keys:
        out_dict[key] = [d[key] for d in metadata]  # Extract values for each key

    return out_dict

def format_data(data):

    formatted = {CHROMA_ID : data[CHROMA_ID]}

    formatted.update(open_metadata(data[CHROMA_METADATA]))

    formatted.update({CHROMA_DOCS : data[CHROMA_DOCS]})

    return formatted

def view_data(data):

    if data[list(data)[0]]: # Checking if there is content within the data

        display_data = format_data(data)

        df = pd.DataFrame.from_dict(display_data)
        st.dataframe(df)

def display_error(msg):

    st.title(msg)

def run():

    load_dotenv(override=True)

    try:
        db_path = os.getenv('PATH_DB') 
    except Exception as e:
        db_path = "chromaDB"

    full_path = os.path.join(db_path) 
    
    if not os.path.exists(full_path):
        full_path = os.path.join('../', db_path) # ../ only if you run the program in the same directory

    if os.path.exists(full_path):

        print(f'Starting client from {full_path}')
        client = setup_client(full_path)

        available_connections = collection_names(client)

        print(f'Available collections: {available_connections}')

        if available_connections:

            chosen_collection = setup_selection(available_connections)

            print(f'Reading from {chosen_collection} collection\n')

            collection = client.get_collection(name=chosen_collection) #reading from selectbox variable will return it's text

            view_data(data=collection.get())

            # Remove Collection button
            if st.button("Remover Coleção"):
                client.delete_collection(name=chosen_collection)

        else:

            display_error(f"No collections found in {full_path}!")

    else:

        display_error(f"No database client found in {full_path}")

    print()

if __name__ == "__main__":
    run()