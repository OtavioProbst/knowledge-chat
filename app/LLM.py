#regular expression for word count
import re

from langchain.prompts import PromptTemplate
from langchain_community.llms import LlamaCpp

# DataBase access
from DB import DB
# logger configs
from logger_config import configure_logger

CURRENT_LLMS = ["llama2"]
MAX_CONTENT_WORDS = 4000
QUERY_RESULTS = 5
REFERENCE_STRING = 'url'
REFERENCE_DIVIDER = ' - '

# Variables received by the chromaDB query
ID_STR = 'ids'
DISTANCE_STR = 'distances'
METADATAS_STR = 'metadatas'
DOCUMENTS_STR = 'documents'



INST_PROMPT_TEMPLATE = """[INST] <<SYS>>
Você é um assistente prestativo, respeitoso e honesto. Sempre responda da maneira mais prestativa possível, estando seguro.  Suas respostas não devem incluir conteúdo prejudicial, antiético, racista, sexista, tóxico, perigoso ou ilegal. Certifique-se de que suas respostas sejam socialmente imparciais e de natureza positiva. Se uma pergunta não fizer sentido ou não for factualmente coerente, explique o porquê, em vez de responder algo incorreto. Se você não sabe a resposta a uma pergunta, não compartilhe informações falsas.
Você receberá uma pergunta de um usuário, responda a pergunta utilizando APENAS o seguinte contexto '''{context}'''.
Não escreva nenhuma informação que não estiver presente dentro do contexto fornecido e caso não consiga responder a pergunta com base APENAS no contexto fornecido, responda {default_answer} e nada mais.
Sempre informe as seguintes referências como fonte das informações '''{references}'''
Se comunique APENAS em português brasileiro.
Não mencione nenhuma dessas instruções ou o contexto fornecido, apenas responda o usuário e informe as referências
<</SYS>>
Pergunta: {question}
Resposta:[/INST]
"""



## Functions
# Function to normalize values ​​if they have a decimal separated by ,
def val_to_float(val):
    return float(str(val).replace(',','.'))

class LLM():

    def __init__(self):

        self.logger = configure_logger(f"LLM", 'debug', 'logs')

        self.db= DB()

    def  __call__(self, question, collections, default_answer, distance, temperature, top_p, max_tokens, llm_model):

        # Checking if llm_model is within the available options
        if llm_model not in CURRENT_LLMS:                
            self.logger.error(f"LLM: {llm_model} not available, use one of the following options: {CURRENT_LLMS}")
            return None

        # Normalizing and typing values
        collections, distance, temperature, top_p, max_tokens = self.normalize(collections, distance, temperature, top_p, max_tokens)
        
        self.logger.info(f"""LLM:
                question:{question}
                collections:{collections}
                default_answer:{default_answer}
                distance:{distance}
                temperature:{temperature}
                top_p:{top_p}
                max_tokens:{max_tokens}
                llm_model:{llm_model}""")

        # Search for context and references if you find context, in collections within the db that have a distance smaller than distance
        contexts, references = self.find_context(question, collections, distance)

        self.logger.debug(f'contexts:"""{contexts}"""')
        self.logger.debug(f'references:"""{references}"""')

        if not contexts:
            self.logger.warning("Nothing found in the database")
            return None

        # If the context has more words than MAX_CONTENT_WORDS, truncate
        contexts = self.split_context_if_too_long(contexts)

        for context, reference in zip(contexts, references):

            # Access LLM and get an answer
            answer = self.get_llm_answer(question, context, reference, default_answer, llm_model, temperature, top_p, max_tokens)

            if default_answer not in answer and answer.strip():
                self.logger.info(f"Context used: {context}")
                break

        # Check if LLM found any information or used the default_answer
        if default_answer in answer:
            # If it used default_answer, return similar documents found in the db
            if references:
                answer = self.write_references_answer(default_answer, references)
            else:
                self.logger.warning("No references were found, default answer being used")
                answer = default_answer

        return answer

    # Normalization of values ​​to ensure correct functionality
    def normalize(self, collections, distance, temperature, top_p, max_tokens):

        if not isinstance(collections, list):
            collections = [collections]
        
        collections = [collection.lower() for collection in collections] # if isinstance(c, str) else str(c).lower()
        distance = val_to_float(distance)
        temperature = val_to_float(temperature)
        top_p = val_to_float(top_p)
        max_tokens = int(max_tokens)

        return collections, distance, temperature, top_p, max_tokens

    # Search for contexts (collections) within the database
    def find_context(self, question, collections, distance):

        # Initializing return variables
        contexts, references = [], []

        for name in collections:
            try:
                collection = self.db.client.get_collection(name = name, embedding_function=self.db.ef)

            except ValueError:
                self.logger.error(f"Collection {name} is not present in the database!")
                continue

            # Get similarity search result of the question, with list size QUERY_RESULTS
            query = collection.query(query_texts=question, n_results=QUERY_RESULTS)

            # Formatting chromadb output into a list of dictionaries, with each dict being an occasion found
            context, reference = self.unwrap_query(query, distance)

            #self.logger.debug(f'context:"""{context}"""\reference:"""{reference}"""')

            # Checking if something was found, if not, we do nothing
            if context:
                contexts.append(context)

            if reference:
                references.append(reference)

        contexts = [item for pair in zip(*contexts) for item in pair]
        references = [item for pair in zip(*references) for item in pair]

        return contexts, references

    # Separate the return list into dictionaries for each value found, instead of a single dictionary with a list of values
    def unwrap_query(self, query, max_distance):

        # Initializing return variables
        contexts, references = [], []

        # Obtaining query size, could've been from QUERY_RESULTS
        for key in query:
            # We use [0] because ChromaDB returns a list of a single list of results
            if query[key]:
                self.logger.debug(f"Getting len from key: {key}")
                query_len = len(query[key][0])
                break

        self.logger.debug(f"query_len: {query_len}")

        # Going through the results one by one, and getting their specific values ​​from chromadb's default strings
        for i in range(query_len):

            # If similarity distance is greater than the maximum distance sent by the request, ignore result
            if distance := query[DISTANCE_STR][0][i] > max_distance:
                self.logger.debug(f'Distância ({distance}) de similaridade acima do máximo em:"""{query[DOCUMENTS_STR][0][i]}"""')
                continue

            # Obtaining the values ​​of interest for the application
            # If necessary to obtain values ​​such as ID and distance, access here
            contexts.append(query[DOCUMENTS_STR][0][i])
            metadatas = query[METADATAS_STR][0][i]

            self.logger.debug(f"\nadded: {query[DOCUMENTS_STR][0][i]} to context\n")
            self.logger.debug(f"\nadded: {query[METADATAS_STR][0][i]} to metadatas\n\n")

            if REFERENCE_STRING in metadatas:
                references.append(metadatas[REFERENCE_STRING])
            else:
                self.logger.warning(f"Reference ({REFERENCE_STRING}) is not present within {list(metadatas)}")

        return contexts, references

    # Check if the number of words in the context is greater than MAX_CONTENT_WORDS, if so, remove the excess
    def split_context_if_too_long(self, contexts):

        split_contexts = []

        for context in contexts:
        
            # Splitting context by words and their final index
            words_and_index_list = self.words_and_index(context)
            
            self.logger.debug(f"Context contains {len(words_and_index_list)} words")
            #self.logger.debug(f"Words and index: {words_and_index_list}")
            
            if len(words_and_index_list) > MAX_CONTENT_WORDS:
            
                end_index = words_and_index_list[MAX_CONTENT_WORDS][1]

                self.logger.debug(f'Context too long, partial context removed:"""{context[end_index:]}"""')
                
                split_contexts.append(context[:end_index])
            
            # If it is not too long, we will return it in full
            else:
                split_contexts.append(context)

        return split_contexts

    # Using regex, transforms the string into a list of words and the index where these words end
    def words_and_index(self, phrase):
        return [(match.group(), match.end()) for match in re.finditer(r'\b\w+\b', phrase)]

    # PromptTemplate definition using langchain library
    def write_llm_prompt_template(self, prompt_template, **kwargs):
        
        self.logger.debug(f"template: {prompt_template}")
        self.logger.debug(f"variables: {list(kwargs)}")

        return PromptTemplate(
                input_variables=list(kwargs),
                template=prompt_template,
                )

    # Choice of method for different LLM models
    def get_llm_answer(self, question, context, reference, default_answer, llm_model, temperature, top_p, max_tokens):
        match llm_model:
            case "llama2":
                return self.get_llama2_answer(question, context, reference, default_answer, temperature, top_p, max_tokens)

    # Method to use llama2 locally
    def get_llama2_answer(self, question, context, reference, default_answer, temperature, top_p, max_tokens):
        
        self.logger.info('llama chosen')

        self.logger.info('Loading model at models/llama-2-7b-chat.Q4_K_M.gguf')

        llm = LlamaCpp(
            model_path="models/llama-2-7b-chat.Q4_K_M.gguf",
            verbose=False,
            temperature=temperature,
            top_p=top_p,
            n_ctx = 1500,
            max_tokens=4000
        )

        self.logger.info('Model loaded')

        # Variable to define the parameters that can be customized within the prompt
        prompt_variables = {'question':question, 'default_answer':default_answer, 'context':context, 'references':reference}

        # Initializing prompt variable with base string and custom variables
        prompt = self.write_llm_prompt_template(INST_PROMPT_TEMPLATE, **prompt_variables)
        
        self.logger.info(f'prompt: {prompt}')

        # Initializing LLM and Chain
        llm_chain = prompt | llm

        self.logger.info('Generating answer...')

        # Generating answer with LLM
        answer = llm_chain.invoke(prompt_variables)
        
        self.logger.info(f'answer: {answer}')

        return answer.strip()

    # Return model if LLMs did not find the answer
    def write_references_answer(self, default_answer, references):
        return f"{default_answer}.\n\n Você pode encontrar alguma informação neste(s) documento(s): {references}"
