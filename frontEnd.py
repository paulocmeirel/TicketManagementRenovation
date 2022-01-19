#Bibliotecas

# NATIVAS
import sqlite3
from pathlib import Path
from datetime import datetime

# INTERNAS
import uteis
import conecta_banco
import main

# EXTERNAS
import pandas as pd
import streamlit as st


# TITULO
st.title('Ticket Management Renovation')
st.sidebar.title('Sobre')

# UPLOAD DO ARQUIVO
st.markdown('Fazer upload do arquivo do FrontApp')
arquivo = st.file_uploader("Upload do arquivo", type=['csv','xlsx'])

if arquivo is not None:
    # relatorioExterno = pd.read_csv(arquivo)
    try:
        relatorioExterno = pd.read_csv(arquivo)
    except:
        relatorioExterno = pd.read_excel(arquivo)

    # ATIVAÇÃO DO BACK-END
    START = main.automacaoHandover()
    START.orquestrador(relatorioExterno)


# INFORMAÇÃO DA ÁREA
st.sidebar.markdown('SOBRE OS DESENVOLVEDORES\n'
            '\nEste aplicativo tem como objetivo auxiliar no Ticket Manager utilizando o FrontApp\n'
            '\nSugestões ou críticas, entrar em contato com:\n'
            '\nDesenvolvedor: Paulo Meirelles\n'
            '\nContato: paulo.meirelles@tabas.com.br\n'
            '\nGerente: Pedro Lé\n'
            '\nContato: pedro.le@tabas.com.br\n'
            '\nTime: Growth\n'
            '\nVersão: 1.0')


