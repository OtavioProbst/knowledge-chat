import streamlit as st
import time
import os
import bcrypt

from dotenv import load_dotenv
load_dotenv(override=True)

from logger_config import configure_logger
logger = configure_logger(f'CH', 'debug', 'logs')

from exceptions import BaseError

from WebCrawler import WebCrawler
from PageScraper import PageScraper
from LLM import LLM
from chroma_viewer import run as cv_run

USERNAME = os.getenv('USERNAME')
HASH_PASSWORD = os.getenv('PASSWORD').encode()

# Define the function to create a page
def admin_page():

    st.title("Página ADMIN")

    if 'isAdmin' not in st.session_state:
        st.session_state.isAdmin = False

    if st.session_state.isAdmin is False:
        # Credential Verification with login

        # Placeholders to delete the fields after successfull login
        username_placeholder = st.empty()
        password_placeholder = st.empty()
        login_placeholder = st.empty()

        # Getting user inputs for credentials
        username = username_placeholder.text_input("Username: ")
        password = password_placeholder.text_input("Password: ", type="password")

        # Creating a login button
        login_clicked = login_placeholder.button("Login")

        if login_clicked:
            # Checking if credentials match env variables, bcrypt checks the encripted password
            if ( ( USERNAME ==  username) and bcrypt.checkpw(password.encode(), HASH_PASSWORD) ):
                # Now we set isAdmin to true to keep user "logged in"
                st.session_state.isAdmin = True
                # Then we delete the placeholders, clearing the fields of admin login
                username_placeholder.empty()
                password_placeholder.empty()
                login_placeholder.empty()

            # We check if user actually typed a username and a password to inform it's an incorrect login
            elif username and password:
                st.write("Login inválido!")


    if st.session_state.isAdmin: # can't be else or streamlit needs to load page again to show new pages

        st.empty()

        st.sidebar.title("Módulos")

        pages = {
            "Crawler" : crawler_page, 
            "Scraper" : scraper_page, 
            "LLM" : llm_page, 
            "Chroma Viewer" : chroma_viewer_page
        }
        
        selection = st.sidebar.radio("Ir para", pages.keys())

        pages[selection]() # Calls the selected page function


# FOR WEBCRAWLER
MAX_DEPTH = 10
PPGIA_URL = 'https://www.ppgia.pucpr.br/pt'
REQUEST_DELAY = 1

PPGIA_IGNORE = [
    "/files/papers/",
    "/~jean.barddal/",
    "/en/arquivos/pesquisa/engsoft/",
    "/pt/arquivos/doutorado/teses/",
    "/pt/arquivos/mestrado/dissertacoes/",
    "/pt/arquivos/pesquisa/engsoft/",
    "/pt/arquivos/seminarios/",
    "/cdn-cgi/l/email-protection",
    "/~santin/",
    "/opportunities/thi",
    "/mapa-pucpr.pdf"
]

def crawler_page():

    st.title('Crawler')

    crawl_button_clicked = st.button("Iniciar Crawling")

    url = st.text_input("URL a ser vasculhada*", PPGIA_URL)
    max_depth = int(st.text_input("Profundidade máxima*", MAX_DEPTH))

    handle_crawler_session_state(url)
    
    # Checking if advanced settings button has been pressed, if so, toggle value from session_state
    if st.button('Configurações Avançadas'):
        st.session_state.advanced_settings_button_pressed = not st.session_state.advanced_settings_button_pressed  

    # Checking the current value in session_state to properly toggle between seeing and not seeing advanced settings inputs
    if st.session_state.advanced_settings_button_pressed:
        handle_crawler_adv_set(url)

    if crawl_button_clicked:
        handle_crawling(url, max_depth, st.session_state.ignore_list)

def handle_crawler_session_state(url):
    # Adding advanced settings button config to streamlit session_state
    if 'advanced_settings_button_pressed' not in st.session_state:
        st.session_state.advanced_settings_button_pressed = False  

    # Adding ignore list to session_state to remove URLs by button click
    if 'ignore_list' not in st.session_state:
        # If chosen url is the same as BASE_URL, we add PPGIA_IGNORE to ignore_list
        if url == PPGIA_URL:
            st.session_state.ignore_list = PPGIA_IGNORE
        else:
            st.session_state.ignore_list = []

def handle_crawler_adv_set(url):

    # If chosen url is the same as BASE_URL, we show default IGNORE
    if url == PPGIA_URL:
        st.selectbox("URLs do PPGIA ignoradas por default:", PPGIA_IGNORE)

    ignore_input = st.text_input('Adicionar URLs a ignorar (apenas os caminhos após o domínio ex: /admin/login/), separar por vírgula cada entrada: ')

    # Adding URLs to be ignored
    if st.button('Adicionar'):
        if ignore_input:
            logger.debug(f'Adicionando a ignore: {ignore_input}')
            # Remove " or ' from the entry
            ignore_input = ignore_input.replace('"','').replace("'",'')
            # Split into a list by each , and strip whitespaces
            st.session_state.ignore_list.extend([single_ignore.strip() for single_ignore in ignore_input.split(',')])
            # Clearing input
            ignore_input = ''

    ignore_remove = st.selectbox("URLs a ignorar:", st.session_state.ignore_list)

    if st.button('Remover'):
        try:
            logger.debug(f'Removed de ignore: {ignore_remove}')
            st.session_state.ignore_list.remove(ignore_remove)
        except ValueError:
            st.write(f'{ignore_remove} não está na lista')

def handle_crawling(url, max_depth, ignore_list):
    # Web Crawler

    logger.info(f"Crawling from: {url} with depth: {max_depth} with delay {REQUEST_DELAY}")

    start = time.time() # timing crawl

    crawler = WebCrawler(
        url, 
        max_depth=max_depth, 
        request_delay=REQUEST_DELAY, 
        ignore=ignore_list
    ) # initializing crawler class with url

    logger.debug(f"WebCrawler initialization took {time.time() - start:.2f}s")

    start = time.time() # timing crawl

    crawling_progress = crawler() # crawler call will start crawling

    pages = handle_progress_bar(crawling_progress, max_depth)

    logger.info(f"Crawling took {(time.time() - start)/60:.2f}m")
    
    logger.info(f"Crawled {len(pages)} pages")
    st.write(f"Foram vasculhadas {len(pages)} páginas!")

    st.write(f"Pasta de armazenamento das páginas: {crawler.fh.directories['domain']}")
    
    if 'data_directories' not in st.session_state:
        st.session_state.data_directories = crawler.fh.directories
        
def handle_progress_bar(crawling_progress, max_depth):
        progress_bar = st.progress(0)
        progress_bar_section = 1/max_depth if max_depth > 0 else 1

        # Using yield generator from WebCrawler to show a progress bar on user interface
        for pages in crawling_progress:
            # If it isn't a dict, we haven't reached end of operation
            if not isinstance(pages, dict):
                # If it returned a dictionary, it's the first yield that gives us the current depth and how many URLs this depth has
                if isinstance(pages, tuple):
                    depth, urls = pages
                # If it isn't a tuple, it's the counter/urls for the current URL we're visiting
                else:
                    progress_bar.progress( ( (depth-1) / max_depth ) + ( (pages/urls) * progress_bar_section ) )
                    #logger.debug(f"Depth: {depth}/{max_depth} - URLs: {pages+1}/{urls}")
            else:
                # If val is a dictionary, it means we reached the final yield of __call__, which returns the pages dictionary
                # Manually updating progress_bar to 100% for prettier output
                progress_bar.progress(100)
                return pages

# FOR PAGESCRAPER
MAX_PHRASES = 10

def scraper_page():

    data_dir = None
    data_directories = None

    html_cleanup = {
        "secaoPrincipal": [
        "main"
        ],
        "igualCompleto_ID": [
        None
        ],
        "igualCompleto_CLASS": [
        None
        ],
        "igualParcial_ID": [
        None
        ],
        "igualParcial_CLASS": [
        None
        ]
    }

    st.title('Scraper')

    scraper_button_clicked = st.button("Iniciar Parsing")

    data_dir, data_directories = handle_scraper_session_state()

    context = st.text_input("Contexto de armazenamento*:", st.session_state.context)

    # Updating session state to context inputted
    st.session_state.context = context

    max_depth, max_phrases, html_cleanup = handle_scraper_adv_set(html_cleanup)

    if scraper_button_clicked:
        handle_scraping(data_dir, data_directories, max_depth, html_cleanup, max_phrases, st.session_state.context)

def handle_scraper_session_state():
    # Checking if context is in session_state for ease of use
    if 'context' not in st.session_state:
        st.session_state.context = ''

    # Checking if crawling was done in this session, so we can use the directories created by it
    if 'data_directories' not in st.session_state:
        data_dir = st.text_input("Indique a pasta onde estão armazenados as páginas HTML*")
        data_directories = None
    else:
        st.write(f"Pasta de arquivos HTML: {st.session_state.data_directories['domain']}")
        data_dir = None
        data_directories = st.session_state.data_directories

    # Checking if we have added advanced config button to session_state
    if 'scraper_adv_conf_button' not in st.session_state:
        st.session_state.scraper_adv_conf_button = False

    return data_dir, data_directories

def handle_scraper_adv_set(html_cleanup):
    # Advanced Configs

    scraper_adv_conf_button = st.button("Configurações Avançadas")

    # If button was pressed, we toggle the session_state value
    if scraper_adv_conf_button:
        st.session_state.scraper_adv_conf_button = not st.session_state.scraper_adv_conf_button

    # Checking if the value is true so we can show the advanced config text inputs
    if st.session_state.scraper_adv_conf_button:

        max_depth = int(st.text_input("Escolha a profundidade máxima:", MAX_DEPTH))
        max_phrases = int(st.text_input("Escolha em quantas frases devem ser separadas as entradas no banco de dados:", MAX_PHRASES))

        st.write('')
        st.write('Para múltiplos valores, separe por vírgula.') 
        st.write('Escolha o valor para a limpeza de dados em:')

        for key in html_cleanup: #BUG test if empty inputs will apply default filters
            value = st.text_input(f"{key}", *html_cleanup[key])
            html_cleanup[key] = value.split(',') if value else [value]

    else:
        # Applying default values if not changed in advanced config
        max_depth = MAX_DEPTH
        max_phrases = MAX_PHRASES

    return max_depth, max_phrases, html_cleanup

def handle_scraping(data_dir, data_directories, max_depth, html_cleanup, max_phrases, context):
    # Page Scraper

    start = time.time()

    try:
        scraper = PageScraper(
            pages=None, 
            data_dir=data_dir,
            data_directories=data_directories,
            max_depth=max_depth
        )

    except BaseError as e:
        st.write(f"Erro ao inicializar o scraper: {e}")
        return

    logger.debug(f"PageScraper initialization took {time.time() - start:.2f}s")

    start = time.time()

    try:
        output, code = scraper(html_cleanup, max_phrases, context) # scrape pages
    except BaseError as e:
        st.write(f"Erro ao realizar scraping: {e}")
        return

    logger.info(f"Parsing took {(time.time() - start)/60:.2f}m")
    logger.debug(f"Scraper output: {output}")
    logger.debug(f"Scraper code: {code}")

    st.write("Scraper OK" if code == 200 else f"Erro em Scraper: code={code}")

# FOR LLM
llm_configs = {
    "default_answer" : "Não encontrei a resposta para sua pergunta.",
    "distance" : 1.5,
    "temperature" : 0.7,
    "top_p" : 0.95,
    "max_tokens" : 400,
    "llm_model" : "llama2"
}

def llm_page():

    st.title("Configurações para LLM (Chatbot)")

    handle_llm_session_state()

    handle_llm_configs()

def handle_llm_session_state():
    # Checking if context is in session_state for ease of use
    if 'context' not in st.session_state:
        st.session_state.context = ''

    # Checking if llm_configs is in session_state to replicate same values into Chatbot page
    if 'llm_configs' not in st.session_state:
        st.session_state.llm_configs = llm_configs

def handle_llm_configs():

    st.session_state.context = st.text_input(f"Contexto*", st.session_state.context)

    logger.debug(f'llm context: {st.session_state.context}')

    temp_llm_configs = st.session_state.llm_configs.copy()

    for key, value in temp_llm_configs.items():

        original_type = type(value)

        new_value = st.text_input(f"{key}", value)

        if isinstance(original_type, float):
            new_value.replace(',','.')

        temp_llm_configs[key] = original_type(new_value)

    if st.button('Atualizar valores'):
        st.session_state.llm_configs = temp_llm_configs.copy()

    logger.debug(f'llm configs: {st.session_state.llm_configs}')

def chroma_viewer_page():
    cv_run()

#FOR CHATBOT
def chatbot_page():
    st.title("Página Chatbot")

    handle_chatbot_session_state()

    question = st.chat_input("Qual a sua dúvida?")

    if question:
        handle_llm_qa(question, st.session_state.context)

def handle_llm_qa(question, collections):

    write_chatbox()
    
    msg = {"role": "user", "content": question}
    st.session_state.messages.append(msg)
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

    with st.chat_message("bot"):
        message_placeholder = st.empty()

        start = time.time()
        
        llm = LLM()

        logger.debug(f"LLM initialization took {time.time() - start:.2f}s")

        start = time.time()

        answer = llm(question, collections, **st.session_state.llm_configs)

        logger.info(f"LLM took {(time.time() - start)/60:.2f}m")
        logger.info(f"LLM answer: {answer}")

        msg = {"role": "bot", "content": answer}
        st.session_state.messages.append(msg)
        message_placeholder.markdown(answer)

def write_chatbox(): #BUG rewrites all previous messages each time 
    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        msg = st.chat_message(message["role"])
        msg.markdown(message["content"], unsafe_allow_html=True)

def handle_chatbot_session_state():
    if 'context' not in st.session_state:
        st.session_state.context = ''

    # Checking if llm_configs is in session_state to replicate same values into Chatbot page
    if 'llm_configs' not in st.session_state:
        st.session_state.llm_configs = llm_configs

    if 'messages' not in st.session_state:
        st.session_state.messages = []


# Define main function to control page selection
def main():

    st.sidebar.title("Navegação")

    pages = {
        "Página Chatbot" : chatbot_page, 
        "Página ADMIN" : admin_page
    }

    selection = st.sidebar.radio("Ir para", pages.keys())

    pages[selection]() # Calls the selected page function

if __name__ == "__main__":
    main()