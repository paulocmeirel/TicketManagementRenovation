#Bibliotecas

# NATIVAS
import sqlite3
from pathlib import Path
from datetime import datetime

# INTERNAS
import uteis
import conecta_banco

# EXTERNAS
import pandas as pd

class automacaoHandover():

    def __init__(self):

        # INFORMAÇÕES PARA O GSHEET
        self.credencial = Path(Path.home(), 'Documents','Git',Path('TicketManagementRenovation','keys.json'))
        self.aba = 'Pendências'
        self.id_sheet = '1w98HhzQ8XgYOO-wwKYApfrlYQdPOyIewFkvG8-9VBYM'

        # EMAIL DE COMPARTILHAMENTO
        # gsheets@testecalendario-331017.iam.gserviceaccount.com

        # DADOS DB
        self.db = Path(Path.home(), "Documents",Path("DBs","dbTicketManager.db"))
        self.conn = sqlite3.Connection(self.db)

        # NOME DAS COLUNAS DO DATAFRAME
        self.colunas = ['ID','Data','ID Apart','Endereço','Origem','Ambiente','Tipo','Pendencias','Responsável']

        # LISTA DE PROBLEMAS
        self.problemas = []
        self.log = []

        # DATAFRAME DE MENSAGENS
        #self.mensagens = pd.read_csv(Path(Path.home(), Path("Downloads","mensagens_whatsapp.csv")))

        # SUBINDO O WORKSHEET DA ABA PENDENCIAS - TRATAMENTO
        self.dataframe, self.wks = uteis.gsheets_to_dataframe(self.credencial, self.id_sheet, self.aba)
        self.columns = list(self.dataframe.iloc[0])
        self.dataframe = self.dataframe[1:]
        self.dataframe.columns = self.columns

        # SUBINDO WORKSHEET DA ABA LOG
        self.abaLog = 'Log'
        self.dataframeLog, self.wksLog = uteis.gsheets_to_dataframe(self.credencial, self.id_sheet, self.abaLog)

    @staticmethod
    def localizaApartamento(mensagem):

        try:
            mensagem = mensagem.split('#')[1]
            return mensagem
        except:
            return mensagem

    @staticmethod
    def localizaProblema(mensagem):

        try:
            mensagem = mensagem.split('#')[-1]
            return mensagem
        except:
            return mensagem

    @staticmethod
    def localizaHora(mensagem):

        try:
            mensagem = mensagem.split(' ')[-1]
            return mensagem
        except:
            return mensagem

    @staticmethod
    def localizaData(mensagem):

        try:
            mensagem = mensagem.split(' ')[0]
            return mensagem
        except:
            return mensagem


    def orquestrador(self, arquivo):

        for i in range(len(arquivo)):
            auxiliar = []
            auxiliar1 = []
            try:
                if '#' in arquivo['Extract'][i]:
                    #auxiliar.append(mensagens['Message ID'][i])
                    id = automacaoHandover.localizaData(arquivo['Message date'][i].replace(" ","")) + \
                         automacaoHandover.localizaApartamento(arquivo['Extract'][i].replace(" ","")) + \
                         automacaoHandover.localizaProblema(arquivo['Extract'][i]).replace(" ","")
                    auxiliar.append(id) # ID
                    auxiliar1.append(id)
                    data = automacaoHandover.localizaData(arquivo['Message date'][i]).replace(" ","")
                    auxiliar.append(data) # Data - Coluna B
                    apartamento = automacaoHandover.localizaApartamento(arquivo['Extract'][i]).replace(" ","")
                    auxiliar.append(apartamento)  # Apartamento - Coluna C
                    auxiliar1.append(apartamento)
                    auxiliar.append('') # Endereçopppppppppp
                    auxiliar.append('FrontApp') # Coluna E
                    auxiliar.append('') # Precisa saber o ambiente - Coluna F
                    auxiliar.append('') # Precisa saber o tipo - Coluna G
                    #auxiliar.append(localizaHora(mensagens['Message date'][i])) # Hora
                    problema = automacaoHandover.localizaProblema(arquivo['Extract'][i])
                    auxiliar.append(problema)  # Problema(Pendencia) - Coluna H
                    auxiliar1.append(problema)
                    auxiliar1.append('Inserção')
                    auxiliar1.append('')
                    agora = datetime.now().replace(microsecond=0).strftime("%d/%m/%Y %H:%M:%S")
                    auxiliar1.append(agora)
                    auxiliar1.append('')
                    #responsavel = self.mensagens['Attributed to'][i].replace(" ","")
                    #auxiliar.append(responsavel) # Atribuido(Responsável) - Coluna I
                    auxiliar.append('') # Atribuido(Responsável) - Coluna I
                    self.problemas.append(auxiliar)
                    self.log.append(auxiliar1)

            except Exception as ex:
                print(ex)

        """
        for i in range(len(self.mensagens)):
            auxiliar = []
            auxiliar1 = []
            try:
                if '#' in self.mensagens['Extract'][i]:
                    #auxiliar.append(mensagens['Message ID'][i])
                    id = automacaoHandover.localizaData(self.mensagens['Message date'][i].replace(" ","")) + \
                         automacaoHandover.localizaApartamento(self.mensagens['Extract'][i].replace(" ","")) + \
                         automacaoHandover.localizaProblema(self.mensagens['Extract'][i]).replace(" ","")
                    auxiliar.append(id) # ID
                    auxiliar1.append(id)
                    data = automacaoHandover.localizaData(self.mensagens['Message date'][i]).replace(" ","")
                    auxiliar.append(data) # Data - Coluna B
                    apartamento = automacaoHandover.localizaApartamento(self.mensagens['Extract'][i]).replace(" ","")
                    auxiliar.append(apartamento)  # Apartamento - Coluna C
                    auxiliar1.append(apartamento)
                    auxiliar.append('') # Endereçopppppppppp
                    auxiliar.append('FrontApp') # Coluna E
                    auxiliar.append('') # Precisa saber o ambiente - Coluna F
                    auxiliar.append('') # Precisa saber o tipo - Coluna G
                    #auxiliar.append(localizaHora(mensagens['Message date'][i])) # Hora
                    problema = automacaoHandover.localizaProblema(self.mensagens['Extract'][i])
                    auxiliar.append(problema)  # Problema(Pendencia) - Coluna H
                    auxiliar1.append(problema)
                    auxiliar1.append('Inserção')
                    auxiliar1.append('')
                    agora = datetime.now().replace(microsecond=0).strftime("%d/%m/%Y %H:%M:%S")
                    auxiliar1.append(agora)
                    auxiliar1.append('')
                    #responsavel = self.mensagens['Attributed to'][i].replace(" ","")
                    #auxiliar.append(responsavel) # Atribuido(Responsável) - Coluna I
                    auxiliar.append('') # Atribuido(Responsável) - Coluna I
                    self.problemas.append(auxiliar)
                    self.log.append(auxiliar1)

            except Exception as ex:
                print(ex)"""

        # INPUT NA TABELA
        #
        j = 0
        query = 'SELECT ID FROM TBL_HANDOVER'
        listaLog = pd.read_sql_query(f"{query}", self.conn)
        for i in range(len(self.dataframe)):
            try:
                if self.dataframe.iloc[i,0] == '':
                    #self.wks.update_values(f'A{i + 3}', [self.problemas[j]])
                    #j += 1
                    if self.problemas[j][0] in list(listaLog['ID']):
                        j += 1
                    else:
                        self.wks.update_values(f'A{i+3}',[self.problemas[j]])
                        j += 1
            except:
                pass

        # INPUT NO DB
        try:
            camposProblema = ['ID', 'DATA', 'ID_APART', 'PROBLEMA']
            dfProblemas = pd.DataFrame(self.problemas, columns=self.colunas)
            dfProblemas.drop(['Endereço','Origem','Ambiente','Tipo','Responsável'],axis = 1, inplace=True)
            tabelaProblema = 'TBL_HANDOVER'
            for i in range(len(dfProblemas)):
                try:
                    conecta_banco.input_values(self.db, tabelaProblema, tuple(camposProblema), dfProblemas.loc[i])
                except Exception as ex:
                    logProblema = 'TBL_LOG'
                    camposLog = ['ID','DATA','PROBLEMA']
                    tempoLog = datetime.now()
                    conecta_banco.input_values(self.db, logProblema, tuple(camposLog), [dfProblemas.loc[i][0],tempoLog,ex])

        except Exception as ex:
            print(ex)

        # INPUT NA TABELA LOG

        j = 0
        for i in range(len(self.dataframeLog)):
            try:
                if self.dataframeLog.iloc[i,0] == '':
                    if self.log[j][0] in list(listaLog['ID']):
                        j += 1
                    else:
                        self.wksLog.update_values(f'A{i+2}',[self.log[j]])
                        j += 1
            except:
                pass



if __name__ == '__main__':
    START = automacaoHandover()
    START.orquestrador()

# ATENDENTE É A COLUNA N
# ORIGEM DO CHAMADO COLUNA F
# ORIGEM DO CHAMADO COLUNA D (DE FORA OU DO PMO)
