#database library for semantic search
import chromadb
from chromadb.utils import embedding_functions

#importing environ variables
import os
from dotenv import load_dotenv
import uuid

from logger_config import configure_logger

# Obtaining environment variables for default database definition
load_dotenv(override=True)

try:
    PATH_DB = os.getenv('PATH_DB') 
except Exception as e:
    PATH_DB = "chromaDB"
try:
    MODEL_NAME = os.getenv('MODEL_NAME')
except Exception as e:
    MODEL_NAME = "sentence-transformers/distiluse-base-multilingual-cased-v1"

#Fixed names from chromadb
CHROMA_ID = "ids"
CHROMA_METADATA = "metadatas"

# Values to organize data within ChromaDB
URL_KEY = 'url'
SECTION_KEY = 'section'

class DB():
    def __init__(self, path=PATH_DB, model_name=MODEL_NAME):

        self.logger = configure_logger(f'DB', 'debug', 'logs')
        self.logger.info(f"Loading database at {path}")

        # Initializing client and embedding function for Chromadb Database
        self.client = chromadb.PersistentClient(path=path)

        self.ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=model_name)

    def prepare_for_db(self, data:dict, max_phrases):

        # Function to divide content into sentences to facilitate database searching

        output = {}

        for metadata, content in data.items():

            if content is None:
                self.logger.warning(f"Content in {metadata} is None!")
                continue

            if not isinstance(metadata, str):
                self.logger.warning(f"{metadata} is not a string type, it is: {type(metadata)}")
                continue

            sectioned_content = []

            # Dividing into sentences if the sentence is not null
            phrases = [phrase for phrase in content.split('\n') if phrase.strip()]

            # If there are more phrases than max_phrases, we combine them each max_phrases
            while len(phrases) >= max_phrases:
                sectioned_content.append('\n'.join(phrases[:max_phrases]))
                phrases = phrases[max_phrases:]

            # Append what was missing
            if phrases and len(phrases) < max_phrases: # redundante?
                sectioned_content.append('\n'.join(phrases))

            # Adding the metadata and split content to the output
            output[metadata] = sectioned_content

            self.logger.debug(f"Content of {metadata} divided into {len(sectioned_content)} parts")

        return output
    
    def create_collection(self, context):
        return self.client.get_or_create_collection(name = context, embedding_function=self.ef)

    def store_in_db(self, data, collection):

        """
        Data variable must be a dictionary of URL keys with their values in list format { url : [content] }
        """


        # Getting the data currently present within the collection
        collection_data = collection.get()

        # Get the IDs that have already been used
        current_ids = collection_data[CHROMA_ID]

        self.logger.info(f"Starting storage at:{collection.name}")

        # Getting size of data dictionary to be stored
        urls_ammount = len(data)

        # Accessing the URLs and already divided content of each page
        for j, (url, content) in enumerate(data.items()):

            # log to represent storage progress
            self.logger.debug(f"{j+1}/{urls_ammount}") #ADD for deep debugging, too much data for normal usage

            for i, sectioned_content in enumerate(content):

                id = str(uuid.uuid5(uuid.NAMESPACE_DNS, sectioned_content))

                if id in current_ids:

                    # Getting position in the list of IDs
                    pos = current_ids.index(id)

                    # Obtaining the metadata from the obtained position
                    metadata = collection_data[CHROMA_METADATA][pos]

                    # Checking if URL has not already been stored in this collection
                    if url in metadata[URL_KEY]:
                        break

                    # Adding new data to the stored metadata
                    metadata[URL_KEY] = f"{metadata[URL_KEY]} , {url}"
                    metadata[SECTION_KEY] = f"{metadata[SECTION_KEY]} , {i}"

                else:
                    metadata = {
                        URL_KEY : url,
                        SECTION_KEY : i
                    }

                    # Adding new values ​​to comparison variables
                    current_ids.append(id)
                    collection_data[CHROMA_METADATA].append(metadata)

                try:
                    collection.upsert(ids=id, documents=sectioned_content, metadatas=metadata)
                except Exception as e:
                    return e, 400

        self.logger.info("Finished storage.")

        return {}, 200