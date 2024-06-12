import os
from pathlib import Path # easy directory path creation
from datetime import datetime # getting today's date
import json
import uuid
import base64
from requests_html import HTML

from exceptions import BaseError
from logger_config import configure_logger

# FILE STORAGE CONFIGURATIONS
BASE_DATA_DIR = 'data'
DOMAIN_DIRECTORY = 'domain'
PAGES_DIRECTORY = 'pages'
FILES_DIRECTORY = 'files'
JSON_DIRECTORY = 'json'
LOGS_DIRECTORY = 'logs'


from datetime import datetime # getting today's date
TODAY = datetime.today().strftime('%Y_%m_%d')

# String for pages jsons
URL_KEY = 'url'
DEPTH_KEY = 'depth'
CONTENT_KEY = 'content'

def string_to_base64(input_string):
    # Encode the string to bytes
    bytes_data = input_string.encode('utf-8')

    # Use base64 encoding
    encoded_data = base64.b64encode(bytes_data)

    # Convert bytes back to a string
    result_string = encoded_data.decode('utf-8')

    return result_string

def base64_to_string(base64_string):
    # Decode the Base64 string to bytes
    decoded_bytes = base64.b64decode(base64_string)

    # Convert bytes back to a string
    result_string = decoded_bytes.decode('utf-8')

    return result_string

class FileHandler():

    def __init__(self, data_dir=None, directories=None):

        self.directories = {}
        self.url_to_filename = {}

        if directories is None:
            if data_dir is None:
                raise BaseError('ERROR on FileHandler: directories and data_dir are both None')
            self.setup_directories(data_dir)
        else:
            self.directories = directories

        self.logger = self.setup_logger('FH')

    def setup_directories(self, data_dir):
        
        # sub_dir will be the current date, so we update the pages at least once a day
        self.directories[DOMAIN_DIRECTORY] = os.path.join(BASE_DATA_DIR, data_dir, TODAY)

        self.directories[LOGS_DIRECTORY]  = os.path.join(LOGS_DIRECTORY, data_dir, TODAY) # for log keeping

        self.directories[PAGES_DIRECTORY] = os.path.join(self.directories[DOMAIN_DIRECTORY], f"{PAGES_DIRECTORY}/") # for the downloaded html pages
        self.directories[JSON_DIRECTORY]  = os.path.join(self.directories[DOMAIN_DIRECTORY], f"{JSON_DIRECTORY}/" ) # for the jsons used to store important urls

        # if path to file doesn't exist, create it and its parents
        Path(self.directories[PAGES_DIRECTORY]).mkdir(parents=True, exist_ok=True)
        Path(self.directories[JSON_DIRECTORY]).mkdir(parents=True, exist_ok=True)
        Path(self.directories[LOGS_DIRECTORY]).mkdir(parents=True, exist_ok=True)

    def setup_logger(self, logger_name):
        return configure_logger(logger_name, 'debug', self.directories[LOGS_DIRECTORY])

    def load_page(self, url=None, filename=None):

        if os.path.exists(self.directories[PAGES_DIRECTORY]):

            if url:
                filename = str(uuid.uuid5(uuid.NAMESPACE_DNS, url))

            if filename in os.listdir(self.directories[PAGES_DIRECTORY]):

                self.logger.debug(f"Found FILENAME in database: {filename}")

                # File is in json format, so we access it using json.load
                try:
                    with open(os.path.join(self.directories[PAGES_DIRECTORY], filename), 'r', encoding='utf8') as f:
                        data = json.load(f)
                        data[CONTENT_KEY] = base64_to_string(data[CONTENT_KEY])

                except Exception as e:
                    raise BaseError(f"Error on opening and accessing data from json file: {e}")

                # Storing the two values for json
                self.url_to_filename[url] = filename

                return data
            else:
                return None
        else:
            self.logger.error(f"page directory: {self.directories[PAGES_DIRECTORY]} not found!")

        return None
    
    def save_page(self, url, html, current_depth):

        #Function to save webpage we've accessed locally, as to avoid sending requests and needing to wait request_delay in the future

        # Checking if path exists, if not, we create it
        if not os.path.exists(self.directories[PAGES_DIRECTORY]):
            Path(self.directories[PAGES_DIRECTORY]).mkdir(parents=True, exist_ok=True)

        # Checking if URL is valid
        if not url:
            raise BaseError(f"Invalid URL: {url}")

        # Generating a string of a unique ID for each url we save
        filename = str(uuid.uuid5(uuid.NAMESPACE_DNS, url))

        # If html is not a string we need to extract the string from it
        if isinstance(html, HTML):
            html = html.html

        # Data object we store in the json file
        data = {
            URL_KEY : url,
            DEPTH_KEY : current_depth,
            CONTENT_KEY : string_to_base64(html)
        }

        try:
            with open(os.path.join(self.directories[PAGES_DIRECTORY], filename), 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, separators=(',', ': '))

            data[CONTENT_KEY] = base64_to_string(data[CONTENT_KEY])

            self.logger.debug(f"Saved URL in database {url} as {filename}")

        except Exception as e:
            raise BaseError(f"save_page: error: {e}")

        # Storing the two values for json
        self.url_to_filename[url] = filename
            
        return data

    def save_json(self, filename, content):

        if not os.path.exists(self.directories[JSON_DIRECTORY]):
            Path(self.directories[JSON_DIRECTORY]).mkdir(parents=True, exist_ok=True)

        if isinstance(content, set):
            content = list(content)
        
        if isinstance(content, list):
            content = sorted(content)
        elif isinstance(content, dict):
            content = dict(sorted(content.items()))
        else:
            self.logger.warning(f"Failed to save {filename} json. Content wasn't a set or a list or a dict")
            return
        
        self.logger.debug(f"Saving {filename} json")
        
        with open(os.path.join(self.directories[JSON_DIRECTORY], f"{filename}_{len(content)}.json"), 'w', encoding='utf8') as f:
            json.dump(content, f, indent=2, separators=(',', ': '))

    def load_json(self, filename):        
        # Checking if base directory exists where the json files will be located
        if not os.path.exists(self.directories[JSON_DIRECTORY]):
            return None

        # Number after the json filename, representing how many values it has, we want the highest one to get the most ammount of information
        highest_num = 0
        final_file = None

        # We get all files in the directory and look for highest_num file
        for file in os.listdir(self.directories[JSON_DIRECTORY]):
            if file.startswith(filename):
                num = int(file.split(f"{filename}_")[1].split('.json')[0])
                if num > highest_num:
                    highest_num = num
                    final_file = file

        if not final_file:
            return None
        
        self.logger.debug(f"Reading file: {final_file}")

        with open(os.path.join(self.directories[JSON_DIRECTORY], final_file), 'r', encoding='utf8') as f:
            return json.load(f)

    def save_url_to_filename(self):
        self.save_json('url_to_filename', self.url_to_filename)