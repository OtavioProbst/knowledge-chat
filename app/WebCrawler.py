from requests_html import HTMLSession, HTML
from requests.exceptions import HTTPError
#libraries for reading PDF from a web link
from io import BytesIO
from PyPDF2 import PdfReader

import time # for delaying and avoiding DDoS
from urllib.parse import urljoin # concatenating URLs properly
from urllib.parse import urlsplit
from urllib import robotparser

from exceptions import BaseError
from FileHandler import FileHandler


LOG_URL_CLEAN = False # Variable to create logger for URL cleanup
SLASH_REPLACER = '_'
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
#FILES_EXTENSIONS = ('.pdf', '.doc', '.docx', '.zip', '.rar', '.gz', '.csv', '.xlsx', '.xls', '.txt', '.ipynb', '.png')
FILES_EXTENSIONS = ('.doc', '.docx', '.zip', '.rar', '.gz', '.csv', '.xlsx', '.xls', '.txt', '.ipynb', '.png')

# String for pages jsons
from FileHandler import CONTENT_KEY

def get_url_domain(url):
    domain = urlsplit(url).hostname
    if 'www.' in domain:
        domain = domain.split('www.')[1]
    return domain

def get_url_scheme(url):
    return urlsplit(url).scheme or 'https' # Defaulted to https as it's the more common protocol

def format_cookies(cookies):
    return "; ".join( [ f"{cookie['name']}={cookie['value']}" for cookie in cookies ] )

class WebCrawler():

    def __init__(self, base_url:str, max_depth:int, request_delay:int, ignore:list=None):
        self.initialize_values(base_url, max_depth, request_delay, ignore)

        # Getting the domain and scheme of our base url
        self.domain = get_url_domain(self.url)
        self.scheme = get_url_scheme(self.url)

        # Initial setups
        try:
            self.fh = FileHandler(data_dir=self.domain)
        except BaseError as e:
            raise BaseError(f"Error on WS: {e}")

        self.setup_loggers()

        self.logger.info(f"\Iniciando WebCrawler em:\nURL: {self.url}\nProfundidade Máxima:{self.max_depth}\n")
        self.logger.info(f"Scheme: {self.scheme}, Domain: {self.domain}")

        self.setup_session(self.url)
        self.rp = self.setup_robot_parser()

        # Update values using robot parser
        if self.rp:
            self.logger.info("RobotParser active")
            
            new_delay = self.rp.crawl_delay(self.user_agent)
            self.logger.debug(f"New delay: {new_delay}")
           
            new_ignore = self.get_robots_disallow()
            self.logger.debug(f"New ignore: {new_ignore}")
            
            if new_ignore:
                self.ignore += new_ignore
                self.logger.info(f"Updated ignore to {self.ignore}")
            if new_delay:
                self.request_delay = new_delay
                self.logger.info(f"Updated request delay to {self.request_delay}")

        self.load_jsons()

    def initialize_values(self, base_url, max_depth, request_delay, ignore=None):

        self.user_agent = USER_AGENT
        self.headers = None

        self.pages = {} # pages read from url. pages[url] = content
        self.directories = {} # dictionary to store all used directories for files being saved

        self.error_pages = {} # dictionary of url : error_code

        self.outside_domain_urls = set() # set of URLs that were outside the domain of the crawl
        self.outside_max_depth_urls = set() # set of URLs that are farther than max_depth
        self.file_urls = set() # set of URLs that are a file (from FILES_EXTENSIONS), and not an html webpage
        self.unmarked_file_urls = set() # set of URLs that don't end in a file extension, but their content is of a file

        # Initializing base variables
        self.url = base_url
        self.max_depth = max_depth
        self.request_delay = request_delay
        self.ignore = ignore

        # Initializing current_depth for storing in the files we save for each webpage, so we can determine how deep it originally was
        self.current_depth = 0

    def setup_robot_parser(self):

        base_url = f'{self.scheme}://{self.domain}'

        robots_url = urljoin(base_url, 'robots.txt')

        self.logger.debug(f"robots url: {robots_url}")

        try:
            robots_file = self.session.get(robots_url).content.decode('utf-8').splitlines()
        except Exception as e:
            self.logger.error(f"Failed to setup robot parser: {e}")
            return None

        self.logger.debug(f"robots_file: {robots_file}")

        rp = robotparser.RobotFileParser(url=robots_url)
        rp.parse(robots_file)

        return rp

    def get_robots_disallow(self):
        disallow = []

        if self.rp.default_entry:
            disallow += [rule.path for rule in self.rp.default_entry.rulelines if rule.allowance == False]

        if self.rp.entries:
            for entry in self.rp.entries:
                if entry.useragents == self.user_agent:
                    disallow += [rule.path for rule in entry.rulelines if rule.allowance == False]

        return disallow

    def setup_loggers(self):

        # Here we start the loggers from FileHandler

        self.logger = self.fh.setup_logger(f"WC")

        if LOG_URL_CLEAN:
            self.logger_clean = self.fh.setup_logger(f"WC_CLEAN")

    def setup_session(self, url):

        # Here we start an HTMLSession so we can access all webpages using the same configurations and permissions, such as cookies and headers

        self.logger.info(f"Iniciando sessão")

        session = HTMLSession()
        
        # We get the initial response to obtain the cookies for later use
        response = session.get(url)

        cookies = response.cookies

        # Format the cookies to the correct format for headers.update
        cookies = format_cookies(cookies)

        # Initializing headers for the session so then we can use it for all requests
        session.headers.update({
            "User-Agent" : self.user_agent,
            "Cookie": cookies
        })

        # Making it internal to the object
        self.session = session

        self.logger.info(f"Sessão OK")

    def load_jsons(self):
        # Loading json files we can use to get a headstart on our crawl
        self.error_pages = self.fh.load_json("errors") or {}

    def __call__(self):
        # Main function to start WebCrawling

        # Try clause so we can interrupt Crawling manually and store progress without restarting from 0
        try:
            # First webpage will be self.url, from then we check if it was redirected, and grab all new URLs present in it, then we crawl
            if not (redirect_url := self.set_pages_html(self.url)):
                self.logger.error("Failed to crawl base url")
                return {}
            else:
                #First we turn the content into an HTML object
                html_obj = HTML(url=redirect_url, html=self.pages[redirect_url][CONTENT_KEY])
                # We use urljoin for each url in the webpage so that if theres a relative path, it'll combine them together
                # if it's an absolute path, urljoin will not make an invalid URL, and we use a set to remove any repeat URLs
                urls = set( [ urljoin(redirect_url, new_url) for new_url in html_obj.links ] )
                # then we send it to clean_urls, to verify if it matches our criteria to be accessed
                urls = self.clean_urls(urls)
                # finally we crawl it starting at depth=0, with the progress empty
                yield from self.crawl_urls(urls, 1, [])

        except KeyboardInterrupt:
            print(f"Terminating Crawling, now saving files")

        # Storing all that was crawled and not crawled in json files
        self.save_jsons()
        self.fh.save_url_to_filename()

        # Return dictionary with all pages that were crawled and it's contents
        yield self.pages

    def set_pages_html(self, url):

        # Function to add the html page to self.pages[url], will check if there is a redirect and if so, will return the final url with no redirect

        # Bool to mark if we loaded the file, so we don't need to wait for next request
        was_loaded = False

        # Then we try to load page from a saved file
        try:
            data = self.fh.load_page(url)
        except BaseError as e:
            self.logger.warning(f"Error on set_pages_html: {e}")
            return None

        if not data:

            # If there is no file or we failed to load page, we try to access the url
            html = self.access_page(url)
            if not html:
                # If that also failed, return False
                return False
            
            # If we succeeded on accessing the page, we now save it so we can access it later without sending a request
            # Check if page was saved successfully
            else:
                try:
                    data = self.fh.save_page(url, html, self.current_depth)
                except Exception as e:
                    self.error_pages[url] = str(e)

                if not data:
                    return False
        else:
            was_loaded = True
            if data[CONTENT_KEY].strip():
                html = HTML(url=url, html=data[CONTENT_KEY])
            else:
                self.logger.warning(f"Failed to convert {url} to HTML object")
                return False

        # Add the page to our pages dictionary
        self.pages[url] = data
        self.logger.debug(f"added {url} to self.pages") #ADD for domain verification, not needed for daily usage

        # Check if the page is a redirect page
        new_html, new_url = self.find_html_redirect(url, html, was_loaded)

        if new_html is None: # or False?
            #self.logger.debug(f"url {new_url} has no html") #ADD for domain verification, not needed for daily usage
            return url

        if new_url:
            # If there is a redirect, we now repeat this process with the new_url
            return self.set_pages_html(new_url)
        else:
            #self.logger.debug(f"{url} has no new_url") #ADD for domain verification, not needed for daily usage
            if not was_loaded:
                # If there was no new_url we apply the request_delay to not overload the server, 
                time.sleep(self.request_delay)
            # and then return the previous url that was successful
            return url

    def access_page(self, url, as_pdf=False):

        # Check if URL ends with .pdf extension, so we treat it as such
        if url.endswith('.pdf'):
            as_pdf=True

        try:

            self.logger.info(f"#REQUEST:\tAccessing {url} - as_pdf: {as_pdf}")

            # Sending get request to web page
            response = self.session.get(url)
            # Function that raises an exception if request was unsucessful
            response.raise_for_status()

            if as_pdf:

                # The index will correspond to the page number.
                pdf_reader = PdfReader(BytesIO(response.content))

                # Text contained in PDF file
                pdf_text = ''

                #Getting text page by page
                for page in pdf_reader.pages:
                    pdf_text += page.extract_text()

                if not pdf_text:
                    self.error_pages[url] = 'Empty PDF'
                
                return pdf_text
                

            # Determine type of page accessed
            content_type = self.get_content_type(response)

            if content_type != 'html':
                # If type is in FILES_EXTENSIONS, and it wasn't filtered at filter_url, we add it to unmarked_files
                if content_type in FILES_EXTENSIONS:

                    # Handle PDF files here
                    if content_type == '.pdf':
                        self.logger.debug('#REQUEST: Accessing as PDF now')
                        return self.access_page(url, as_pdf=True)

                    self.unmarked_file_urls.add((url, content_type))

                else:
                    self.error_pages[url]  = f"Type of content ({content_type}) from ({url}) isn't in FILES_EXTENSIONS"

                # In case it's not html or .pdf, we return None so we don't access page
                return None

        except HTTPError as e:
            self.logger.warning(f"access_page: HTTPError: {e}")
            # Adding error to our error_pages that will be later converted into a .json with all webpages for verification
            self.error_pages[url] = e.response.status_code
            return None
        
        except Exception as e:
            self.logger.warning(f"access_page: Exception: {e}")
            # Adding error to our error_pages that will be later converted into a .json with all webpages for verification
            self.error_pages[url] = str(e)
            return None

        # If all was ok, return html portion of response
        return response.html

    def find_html_redirect(self, url, html, was_loaded=False):
        if isinstance(html, str):
            self.logger.debug(f"Converted {url} to HTML object, was a string")
            try:
                # Convert html as string to html object
                html = HTML(url=url, html=html)
            except Exception as e:
                self.logger.warning(f"\n\nERROR on find_html_redirect: {e}")
                self.logger.warning(f"URL: {url}")
                self.logger.warning(f"html: '{html}'\n\n\n")
                return False, False

        # Look for the default string for a http redirect 
        redirect = html.find('meta[http-equiv="Refresh"]', first=True)

        # If found a redirect, we need to treat it
        if redirect:
            # Obtain the delay and text portion of the redirect
            delay, text = redirect.attrs["content"].split(";")

            # Check if the text is formatted as expected
            if text.strip().lower().startswith("url="):
                # if so, obtain the url string in the text
                new_url = text.strip()[4:]

                self.logger.debug(f"{url} redirected to {new_url}")

                # If we've already visited this URL, we return the html we've already previously obtained
                if new_url in self.pages:
                    self.logger.error(f"URL ({new_url}) already in pages")
                    return (self.pages[new_url][CONTENT_KEY], new_url)

                # If the new_url is not valid by our clean_url criteria, we return the previous working html
                if (new_url := self.clean_url(new_url)) is None:

                    return (html, None)

                # If we didn't load the webpage locally, we apply the delay present in the redirect
                if not was_loaded:
                    self.logger.debug(f"Sleeping for {delay} on {new_url}")
                    time.sleep(int(delay.strip()))

                # Finally we return the html with the new_url that we need to access
                return (html, new_url)

        # If no redirect, we return the previously working html
        return (html, None)

    def get_content_type(self, request):

        # Function to determine the type of content a webpage has

        content_type = request.headers['Content-Type']

        if 'html' in content_type:
            return 'html'
        if 'xml' in content_type: #XMLParsedAsHTMLWarning: It looks like you're parsing an XML document using an HTML parser. If this really is an HTML document (maybe it's XHTML?), you can ignore or filter this warning. If it's XML, you should know that using an XML parser will be more reliable. To parse this document as XML, make sure you have the lxml package installed, and pass the keyword argument `features="xml"` into the BeautifulSoup constructor.
            return 'html'
        if 'pdf' in content_type:
            return '.pdf'
        if 'png' in content_type:
            return '.png'
        if 'jpg' in content_type:
            return '.jpg'
        if 'json' in content_type:
            return '.json'

        self.logger.warning(f"Tipo de conteúdo não encontrado: {content_type}")

        return None

    def crawl_urls(self, urls, depth, progress):

        # Main crawling function, go through all URLs and obtain the new URLs in them for more crawling

        # Verifying we haven't reached max_depth as determined by initial configs, if so, we terminate crawling
        if depth > self.max_depth:

            self.logger.info(f"#DEPTH:\tNot accessing urls, it'll be too deep")

            self.outside_max_depth_urls.update(urls)

            return

        # Setting current depth so we can add it to each webpage we store locally, so for future uses we know how deep it originally was
        self.current_depth = depth

        # Set for all new urls we obtain for each webpage, update it each loop so then we crawl it after we're done with this depth
        next_urls = set()

        # For checking progress through caller of class
        yield (depth, len(urls))

        for i, url in enumerate(urls):

            # progress and progress_string are solely for log keeping and watching the program run, updating at each step so we know how far we are
            progress_string = f"{i+1}/{len(urls)}"

            # progress will show how many webpages we've crawled on each depth, and then append once we reach a bigger depth
            if len(progress) < depth:
                progress.append(progress_string)
            else:
                progress[depth-1] = progress_string

            # For checking progress through console
            print(f"\rProgress ({depth}/{self.max_depth}): {progress}", end='')

            self.logger.debug(f"Depth: {depth} | {progress}")

            # Verify here if we haven't previously visited the url, if so we can skip it
            if url in self.pages:
                self.logger.debug(f"#REPEAT:\tURL {url} already previously accessed")
                continue

            self.logger.info(f"#CRAWL:\tGetting URLs from {url}\tDepth: {depth}")

            # If for any reason we couldn't add the url to self.pages, this will continue to next URL
            if not (redirect_url := self.set_pages_html(url)):
                continue

            #First we turn the content into an HTML object
            if self.pages[redirect_url][CONTENT_KEY].strip():
                html_obj = HTML(url=redirect_url, html=self.pages[redirect_url][CONTENT_KEY])
            else:
                self.logger.warning(f"Failed to convert {redirect_url} to HTML object")
                continue

            # Obtain unique urls present in each webpage
            current_next_urls = set( [ urljoin(redirect_url, new_url) for new_url in html_obj.links ] )

            #self.logger.debug(f"{depth}<<#>>{redirect_url}<<#>>{current_next_urls}") #ADD for domain verification, not needed for daily usage
            
            # Update main set with each set found
            next_urls.update(current_next_urls)

            yield i

        # For checking progress through console, prettier print
        print()

        # Checking if no new urls were found, if so, we've reached the end of the crawling
        if not next_urls:
            self.logger.info(f"Reached final of crawling at depth {depth}")
            return

        # Clean URLs found based on our crawling criterias
        next_urls = self.clean_urls(next_urls)

        # Finally recursively crawl next depth urls, adding one to depth and updated progress
        yield from self.crawl_urls(next_urls, depth + 1, progress)

    def clean_urls(self, urls):
        # Function that will go through all URLs and verify if it fits our criteria to access it after

        if LOG_URL_CLEAN:
            self.logger_clean.debug(f"\turls to clean: {urls}")

        # Set of urls as to avoid repeat
        clean_urls = set()

        for url in urls:
            # If the return from clean_url wasn't None, we add it to main set
            if clean_url := self.clean_url(url):
                clean_urls.add(clean_url)

        if LOG_URL_CLEAN:
            self.logger_clean.debug(f"cleaned urls: {clean_urls}")

        # We convert to list so we can sort it, we sort it so that it's easier for log keeping and finding pages manually
        return sorted(list(clean_urls))

    def clean_url(self, url):
        # Function that will clean an url based on our criterias:

        if LOG_URL_CLEAN:
            self.logger_clean.debug(f"\t\tcleaning url: {url}")

        # The character "#" in URL means it's an anchor to a section of a webpage, not important to differentiate the content of a webpage
        if url and '#' in url:
            url = url.split('#')[0]

        # If url is invalid, we ignore it
        if not url:
            if LOG_URL_CLEAN:
                self.logger_clean.debug(f"\t\t\tinvalid")
            return

        # Check if we can visit the url via the robotparser
        if self.rp:
            if not self.rp.can_fetch(self.user_agent, url):
                if LOG_URL_CLEAN:
                    self.logger_clean.debug(f"\t\t\tRobot Parser denied access to {url}")
                return

        # Checking if any of the ignore paths in the configuration setup are present in the url, if so we ignore it
        for elem in self.ignore:
            if elem in url:
                if LOG_URL_CLEAN:
                    self.logger_clean.debug(f"\t\t\tignored")
                return

        # Checking if the url is inside the domain of our crawling
        if not self.is_url_inside_domain(url):
            if LOG_URL_CLEAN:
                self.logger_clean.debug(f"\t\t\toutside domain")
            self.outside_domain_urls.add(url)
            return

        # Removing / from the end of our URL until it doesn't have it
        while url and url[-1] == '/':
            url = url[:-1]

        # Checking if URL is a file
        if url.endswith(FILES_EXTENSIONS):
            if LOG_URL_CLEAN:
                self.logger_clean.debug(f"\t\t\tis file")
            # Add URL to our file json so we can see what options we have, for future development
            self.file_urls.add(url)
            return

        # If page is an error page
        if url in self.error_pages: 
            if LOG_URL_CLEAN:
                self.logger_clean.debug(f"\t\t\twas an error page")
            return

        # Checking if we've already accessed this URL
        if url in self.pages:
            if LOG_URL_CLEAN:
                self.logger_clean.debug(f"\t\t\tpreviously visited")
            return 

        if LOG_URL_CLEAN:
            self.logger_clean.debug(f"OK")

        # If all checks were unsucessfull, we return the url
        return url

    def is_url_inside_domain(self, url):
        # Function to verify if URL is within our domain
        # If URL doesn't have www or http(s) in it, it's a relative path
        if ('www' in url) or ('http' in url):
            return self.domain in url
        return True

    def save_jsons(self):

        # Here we have all the .json files we use for easier data management

        self.fh.save_json("files", self.file_urls)
        self.fh.save_json("unmarked_files", self.unmarked_file_urls)
        self.fh.save_json("errors", self.error_pages)
        self.fh.save_json("too_deep", self.outside_max_depth_urls)
        self.fh.save_json("outsider", self.outside_domain_urls)