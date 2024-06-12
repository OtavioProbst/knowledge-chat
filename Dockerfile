#alterado devido a erros com biblioteca symspellpy, testar mais opções futuramente
FROM python:3.10.12

#caso alterar, alterar também em docker-compose.yml
WORKDIR /usr/knowledge_chat 

COPY . .

ENV PYTHONUNBUFFERED=1

RUN pip install -r requirements.txt

CMD streamlit run app/Chatbot.py