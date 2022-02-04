# IMPORTAÇÃO DAS BIBLIOTECAS
# NATIVAS
from datetime import datetime
import sqlite3
from pathlib import Path
import time

# EXTERNAS
import pandas as pd
import schedule

# INTERNAS
import uteis
import conecta_banco

class checklistHO():

    def __init__(self):
        # BAIXANDO A PLANILHA COM AS RESPOSTAS
        self.credencial = Path(Path.home(), 'Documents', 'Git', Path('TicketManagementRenovation', 'keys.json'))
        self.aba = 'Respostas ao formulário 1'
        self.id_sheet = '1sMZ48_xeGff3KtGlJi1GWUNQhu1yFO2ptySTT7jN03c'
        self.dataframe, self.wks = uteis.gsheets_to_dataframe(self.credencial, self.id_sheet, self.aba)

        self.caminhoBanco = Path(Path.home(), "Documents", Path("DBs", "dbTicketManager.db"))
        self.conn = sqlite3.connect(self.caminhoBanco)
        # COLUNAS COM COMENTÁRIOS
        self.problemasComentarios = ['Comentários Gerais Cozinha (Deixar em branco caso não hajam comentários)',
                                'Comentários Gerais Sala (Deixar em branco caso não hajam comentários)',
                                'Comentários Gerais Quarto 01 (Deixar em branco caso não hajam comentários)',
                                'Comentários Gerais Quarto 02 (Deixar em branco caso não hajam comentários)',
                                'Comentários Gerais Quarto 03 (Deixar em branco caso não hajam comentários)',
                                'Comentários Gerais Quarto 04 (Deixar em branco caso não hajam comentários)',
                                'Comentários Gerais Banheiro 01 (Deixar em branco caso não hajam comentários)',
                                'Comentários Gerais Banheiro 02 (Deixar em branco caso não hajam comentários)',
                                'Comentários Gerais Banheiro 03 (Deixar em branco caso não hajam comentários)',
                                'Comentários Gerais Banheiro 04 (Deixar em branco caso não hajam comentários)',
                                'Comentários Gerais Varanda (Deixar em branco caso não hajam comentários)']

        self.comodos = ['Cozinha', 'Sala', 'Quarto 01', 'Quarto 02', 'Quarto 03', 'Quarto 04', 'Banheiro 01', 'Banheiro 02',
                   'Banheiro 03',
                   'Banheiro 04', 'Varanda']


    def orquestradorHO(self):
        # TRATAMENTO DAS TABELAS
        self.dataframe = self.dataframe[self.dataframe['Código do Apartamento']!='']
        self.dataframe = self.dataframe.iloc[:,:-6]
        dataframeProblemas = self.dataframe[self.dataframe.eq('NOK')]
        dataframeProblemas.dropna(axis=1,how='all', inplace=True)
        informacoes = self.dataframe.iloc[:,0:5]
        dataframeProblemas = pd.concat([informacoes,dataframeProblemas], axis = 1)
        dataframeProblemas.reset_index(drop=True,inplace=True)
        self.dataframe.reset_index(drop=True, inplace=True)

        # LISTAS AUXILIARES
        problemas = []
        log = []
        auxiliar = []
        auxiliar1 = []
        comodoIndice = 0
        # SEPARANDO OS PROBLEMAS DAS COLUNAS COM NOK
        for problema in self.problemasComentarios:
            posicao = 0
            for status in self.dataframe[problema]:
                if status != '':
                    dataHora = datetime.strptime(self.dataframe['Carimbo de data/hora'][posicao], '%d/%m/%Y %H:%M:%S')
                    apartamento = self.dataframe['Código do Apartamento'][posicao]
                    id = dataHora.strftime("%Y-%m-%d") + apartamento + self.comodos[comodoIndice]+'-'+status.replace(" ","")
                    agora = datetime.now().replace(microsecond=0).strftime("%d/%m/%Y %H:%M:%S")
                    pendencia = self.comodos[comodoIndice]+' - '+status

                    # APPEND PARA LISTA DE PENDENCIAS (HANDOVER)
                    auxiliar.append(id)  # ID - Coluna A
                    auxiliar.append(dataHora.strftime("%Y-%m-%d"))  # Data - Coluna B
                    auxiliar.append(apartamento)  # Apartamento - Coluna C
                    auxiliar.append('Checklist - HO')  # Coluna D
                    auxiliar.append('')  # COLUNA E - CATEGORIA
                    auxiliar.append(pendencia)  # Problema(Pendencia) - Coluna F
                    auxiliar.append('')  # COLUNA G - OBSERVAÇÃO
                    auxiliar.append('')  # COLUNA H - RESPONSÁVEL
                    auxiliar.append('')  # COLUNA I - NÍVEL DE CRITICIDADE
                    auxiliar.append('')  # COLUNA J - TEMPO DECORRIDO
                    auxiliar.append("Novo")  # COLUNA K - STATUS
                    auxiliar.append('')  # COLUNA L - SALVAR
                    auxiliar.append(agora)  # COLUNA M - DATETIME PARA CALCULAR O TEMPO DECORRIDO DO PROCESSO

                    # APPEND PARA LISTA LOG
                    auxiliar1.append(id)  # ID - Coluna A
                    auxiliar1.append(apartamento)  # Apartamento - Coluna B
                    auxiliar1.append('Checklist - HO') # COLUNA C - ORIGEM
                    auxiliar1.append(pendencia)  # COLUNA D - PENDENCIAS
                    auxiliar1.append('')  # COLUNA E - CATEGORIA
                    auxiliar1.append("Novo")  # COLUNA F - STATUS
                    auxiliar1.append('')  # COLUNA G - VALOR
                    auxiliar1.append(agora)  # COLUNA H - TEMPO DA ATUALIZAÇÃO

                    problemas.append(auxiliar)
                    log.append(auxiliar1)
                    auxiliar = []
                    auxiliar1 = []
                    posicao += 1
            comodoIndice += 1
        auxiliar = []
        auxiliar1 = []

        # SEPARANDO OS PROBLEMAS COM AS COLUNAS QUE POSSUEM COMENTÁRIOS
        for problema in dataframeProblemas:
            posicao = 0
            for status in dataframeProblemas[f'{problema}']:
                if status == 'NOK':
                    try:
                        dataHora = datetime.strptime(dataframeProblemas['Carimbo de data/hora'][posicao], '%d/%m/%Y %H:%M:%S')
                        apartamento = dataframeProblemas['Código do Apartamento'][posicao]
                        id = dataHora.strftime("%Y-%m-%d") + apartamento + problema.replace(" ","")
                        agora = datetime.now().replace(microsecond=0).strftime("%d/%m/%Y %H:%M:%S")

                        # APPEND PARA LISTA DE PENDENCIAS (HANDOVER)
                        auxiliar.append(id)  # ID - Coluna A
                        auxiliar.append(dataHora.strftime("%Y-%m-%d"))  # Data - Coluna B
                        auxiliar.append(apartamento)  # Apartamento - Coluna C
                        auxiliar.append('Checklist - HO')  # Coluna D
                        auxiliar.append('')  # COLUNA E - CATEGORIA
                        auxiliar.append(problema)  # Problema(Pendencia) - Coluna F
                        auxiliar.append('')  # COLUNA G - OBSERVAÇÃO
                        auxiliar.append('')  # COLUNA H - RESPONSÁVEL
                        auxiliar.append('')  # COLUNA I - NÍVEL DE CRITICIDADE
                        auxiliar.append('')  # COLUNA J - TEMPO DECORRIDO
                        auxiliar.append("Novo")  # COLUNA K - STATUS
                        auxiliar.append('')  # COLUNA L - SALVAR
                        auxiliar.append(agora)  # COLUNA M - DATETIME PARA CALCULAR O TEMPO DECORRIDO DO PROCESSO

                        # APPEND PARA LISTA LOG
                        auxiliar1.append(id)  # ID - Coluna A
                        auxiliar1.append(apartamento)  # Apartamento - Coluna B
                        auxiliar1.append('Checklist - HO') # COLUNA C - ORIGEM
                        auxiliar1.append(problema)  # COLUNA D - PENDENCIAS
                        auxiliar1.append('')  # COLUNA E - CATEGORIA
                        auxiliar1.append("Novo")  # COLUNA F - STATUS
                        auxiliar1.append('')  # COLUNA G - RESPONSAVEL
                        auxiliar1.append(agora)  # COLUNA H - TEMPO DA ATUALIZAÇÃO

                        problemas.append(auxiliar)
                        log.append(auxiliar1)
                        auxiliar = []
                        auxiliar1 = []

                    except Exception as ex:
                        print(ex)
                posicao += 1

        # CRIAÇÃO DO DATAFRAME DE PROBLEMA
        colunasTicket = ['ID', 'Data', 'ID Apart', 'Origem', 'Categoria', 'Pendencias', 'Observação', 'Responsável',
                        'Nível de criticidade', 'Tempo Decorrido', 'Status', 'Salvar', 'TempoInsert']
        dfProblemas = pd.DataFrame(problemas, columns = colunasTicket)

        # CRIAÇÃO DO DATAFRAME DE LOG
        colunaLog = ['ID','ID Apart','Origem','Pendencias','Categoria','Status','Responsavel','TempoInsert']
        dfLog = pd.DataFrame(log, columns = colunaLog)

        # DATAFRAME DOS PROBLEMAS QUE JÁ FORAM REGISTRADOS
        query = 'SELECT ID FROM TBL_HANDOVER'
        conferencia = pd.read_sql_query(f"{query}", self.conn)

        # DADOS DO WORKSHEET DA TABELA DE PENDENCIAS DO TICKET
        aba = 'Pendências'
        id_sheetTicket = '1w98HhzQ8XgYOO-wwKYApfrlYQdPOyIewFkvG8-9VBYM'
        abaLog = 'Log'

        # SUBINDO O WORKSHEET DA ABA PENDENCIAS - TRATAMENTO
        ticket, wks = uteis.gsheets_to_dataframe(self.credencial, id_sheetTicket, aba)
        columns = list(ticket.iloc[0])
        ticket = ticket[1:]
        ticket.columns = columns
        ticket.reset_index(drop=True, inplace=True)

        # SUBINDO O WORKSHEET DA ABA DE LOG
        ticketLog, wksLog = uteis.gsheets_to_dataframe(self.credencial, id_sheetTicket, abaLog)

        # LINHAS QUE ESTÃO VAZIAS NA PLANILHA
        linhasVazias = []
        for i in range(len(ticket)):
            if ticket.iloc[i, 0].replace(" ", "") == '':
                linhasVazias.append(i)

        problemasNaoPlotados = []
        # PEGANDO OS PROBLEMAS QUE NÃO FORAM PLOTADOS
        for j in range(len(problemas)):
            if problemas[j][0] not in list(conferencia['ID']):
                problemasNaoPlotados.append(problemas[j])

        # PLOTANDO NA TABELA DO SHEETS
        x = 0
        for problema in problemasNaoPlotados:
            try:
                wks.update_values(f'A{linhasVazias[x] + 3}', [problema])
                x += 1
            except:
                pass

        # INPUT NO DB
        try:
            camposProblema = ['ID', 'DATA', 'ID_APART', 'PROBLEMA']

            dfProblemas.drop(['Categoria', 'Observação', 'Origem', 'Responsável', 'Nível de criticidade',
                              'Tempo Decorrido', 'Status', 'Salvar', 'TempoInsert'], axis=1, inplace=True)
            tabelaProblema = 'TBL_HANDOVER'
            for i in range(len(dfProblemas)):
                try:
                    conecta_banco.input_values(self.caminhoBanco, tabelaProblema, tuple(camposProblema), dfProblemas.loc[i])
                except Exception as ex:
                    conn = sqlite3.Connection(self.caminhoBanco)
                    tempoLog = datetime.now()
                    logProblema = f"""INSERT INTO TBL_LOG (ID, DATA, PROBLEMA) VALUES ('{dfProblemas.loc[i][0]}','{tempoLog}','{ex}')"""
                    cursor = conn.cursor()
                    cursor.execute(logProblema)
                    conn.commit()
                    cursor.close()

        except Exception as ex:
            print(ex)

        # INPUT NA TABELA LOG
        # PEGANDO AS LINHAS VAZIAS NA TABELA LOG
        linhasVazias = []
        logNaoPlotados = []
        for i in range(len(ticketLog)):
            if ticketLog.iloc[i, 0].replace(" ", "") == '':
                linhasVazias.append(i)

        # PEGANDO OS PROBLEMAS QUE NÃO FORAM PLOTADOS
        for j in range(len(log)):
            if log[j][0] not in list(conferencia['ID']):
                logNaoPlotados.append(log[j])

        # PLOTANDO NA TABELA DO SHEETS
        x = 0
        for log in logNaoPlotados:
            try:
                wksLog.update_values(f'A{linhasVazias[x] + 2}', [log])
                x += 1
            except:
                pass

        now = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        print('Término: '+now)

if __name__ == '__main__':
    START = checklistHO()
    START.orquestradorHO()
    schedule.every(1).hours.do(START.orquestradorHO)

    while True:
        schedule.run_pending()
        time.sleep(1)