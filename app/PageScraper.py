from bs4 import BeautifulSoup as bs
import os

from FileHandler import FileHandler, URL_KEY, CONTENT_KEY, DEPTH_KEY, PAGES_DIRECTORY
from DB import DB
from exceptions import BaseError

def css_select_extraction(soup, string):
    # Using CSS to extract IDs that contain the received string in their name
    for el in soup.select(string):
        el.extract()

class PageScraper():

    def __init__(self, pages:dict=None, data_dir:str=None, data_directories:dict=None, max_depth=None):
        
        if max_depth is None:
            self.max_depth = 100
        else:
            self.max_depth = max_depth

        self.fh = FileHandler(data_dir=data_dir, directories=data_directories)

        self.logger = self.fh.setup_logger("PS")

        self.unpack_pages(pages)

        self.db = DB()

    def unpack_pages(self, pages):

        load_pages = True

        if pages:
            load_pages = False
            files = pages.keys()
        else:
            pages = {}
            files = os.listdir(self.fh.directories[PAGES_DIRECTORY])

        self.logger.info(f"pages directories: {self.fh.directories[PAGES_DIRECTORY]}")
        #self.logger.debug(f'files: {files}') #ADD for deep debugging, too much data for normal usage

        if not files:
            raise BaseError(f"Error when reading files on {self.fh.directories[PAGES_DIRECTORY]}")


        for filename in files:
            if load_pages:
                data = self.fh.load_page(filename=filename)
            else:
                data = pages[filename]

            # Verify if page depth is over max_depth
            if data[DEPTH_KEY] > self.max_depth:
                #self.logger.debug(f"file {filename} was over max_depth {self.max_depth}") #ADD for deep debugging, too much data for normal usage
                continue

            # We get the html content from the json object
            html_content = data[CONTENT_KEY]
            url = data[URL_KEY]

            pages[url] = html_content

        if pages:
            self.pages = pages
        else:
            raise BaseError("Error when initializing pages for scraper")

    def __call__(self, html_cleanup, max_phrases, context):
        # Formatting pages to data format        
        data = {}
        for url, html in self.pages.items():
            try:
                # If URL was a pdf, we keep the content intact, can't apply bs operations to it
                if url.endswith('.pdf'):
                    data[url] = html
                else:
                    data[url] = bs(html, 'html.parser')
            except TypeError:
                self.logger.warning(f"TypeError on PageScraper call with url: {url}. Skipping it.")
                continue # redundant

        # Creating collection first to see if theres any errors
        try:
            collection = self.db.create_collection(context)
        except ValueError as e:
            raise BaseError(f'Invalid context name! {e}')

        # Cleaning data
        data = self.cleanup_data(data, html_cleanup)

        data = self.db.prepare_for_db(data, max_phrases)

        # And then store in ChromaDB (self.db) generating IDs and adding the page URL in the metadata
        output, code = self.db.store_in_db(data, collection)

        if code != 200:
            self.logger.warning("ERROR with DB: ", output, code)

        return output, code

    def cleanup_data(self, data, html_cleanup):

        # Function to clean web pages in different ways

        html_cleanup_list = []
        for val in html_cleanup.values():
            html_cleanup_list.append(val)

        # Separating values ​​for HTML filtering
        main_section, full_match_id, full_match_class, partial_match_id, partial_match_class = html_cleanup_list

        self.logger.info(f"""
            main_section : {main_section}
            full_match_id : {full_match_id}
            full_match_class : {full_match_class}
            partial_match_id : {partial_match_id}
            partial_match_class : {partial_match_class}"""
        )

        for url, soup in data.items():
            #self.logger.debug(f"Cleaning up data on {url}")
            if soup:
                if isinstance(soup, bs):

                    # Checking if the list is not empty
                    if main_section:
                        # If it is not empty, but does not contain None, we apply the chosen filter
                        if None not in main_section:
                            soup = self.get_main_section(soup, main_section)
                        # If None was sent inside it, we do not change the soup
                    # If the list is empty, we perform the default operation below
                    else:
                        soup = soup.body

                    if not soup:
                        self.logger.warning(f"soup in {url} was empty")
                        data[url] = ''
                        continue

                    self.remove_full_matches(soup, full_match_class, full_match_id)

                    self.remove_partial_matches(soup, partial_match_class, partial_match_id)

                    # Updating with final soup
                    data[url] = soup.text
                else:
                    self.logger.debug(f"Not a soup object at {url}. Can't apply cleanup features, will remain unchanged")                    
            else:
                self.logger.warning(f"Broken soup at {url}")

        return data

    def get_main_section(self, soup, main_section):
        
        # Testing if main_section is a list of IDs, if not, we convert it to list if it is str
        if not isinstance(main_section, list):
            # Testing if it is a string
            if isinstance(main_section, str):
                main_section = [main_section]
            else:
                self.logger.warning(f"main_section: {main_section} not a list or str, it is {type(main_section)}")
                return False

        full_new_soup = None

        for section in main_section:
            
            self.logger.debug(f"Extracting ID {section}")

            extracted_section = soup.find(id=section)

            if extracted_section:
                self.logger.debug(f"ID found and extracted!")
            else:
                self.logger.debug(f"ID not found, confirm it's an ID and not a class")

            if full_new_soup and extracted_section:
                full_new_soup.append(extracted_section)
            elif extracted_section:
                full_new_soup = extracted_section

        if full_new_soup:
            return full_new_soup
        else:
            return soup.body

    def remove_full_matches(self, soup, class_, id):

        # Using BeautifulSoup to find all elements that have IDENTICAL names

        # Initializing values ​​for search
        # If a non-empty list was sent, the "or" comparison below results in the value on the left, if empty, it results in the value on the right
        class_ = class_ or ['header', 'head', 'top', 'footer', 'foot', 'bottom'] # Common values for header and footer classes in HTML
        id = id or [] 

        # Extracting elements that have their class with names identical to common names
        for element in soup.find_all(class_ = class_ + id):
            element.extract()

    def remove_partial_matches(self, soup, class_, id):

        # Using CSS extraction through BeautifulSoup to remove elements that contain the words WITHIN their name

        # Initializing values ​​for search

        # Base search names for classes
        base_names = ['header', 'top', 'footer', 'bottom']
        # If a non-empty list was sent, the "or" comparison below results in the value on the left, if empty, it results in the value on the right
        class_ = class_ or base_names
        id = id or base_names

        # Formatting lists for correct search format in CSS f'[{type}*="{name}"]'
        class_ = self.format_css_partial_search(class_, 'class')
        id = self.format_css_partial_search(id, 'id')

        # Using CSS to extract Classes and IDs
        for extraction in (class_ + id):
            if extraction:
                css_select_extraction(soup, extraction)

    def format_css_partial_search(self, names, type):
        if isinstance(names, list):
            return [f'[{type}*="{name}"]' for name in names if name]

        elif isinstance(names, str):
            return f'[{type}*="{names}"]'