import webbrowser
import pandas as pd
import pygsheets
import csv

def troca_valores(lista, dict):
    """
        TROCA VALORES DA LISTA PELAS REFERENCIAS QUE ESTÃO NO DICIONARIO

        # ARGUMENTS
            lista                           - Required: lista de valores a ter seus valores trocados (list)
            dict                            - Required: dicionário com os valores de referência (dict)
        # RETURN
            lista                           - Required: lista alterada com os valores trocados (list)
    """
    try:
        for index, value in enumerate(lista):
            if value in dict:
                lista[index] = dict[value]
        return lista

    except Exception as ex:
        print('troca_valores')
        print(ex)

def linha_df(string, array):
    """
        VOLTA O INDEX DA LINHA A QUAL TEM A STRING COMO REFERENCIA

         # ARGUMENTS
            string                          - Required: string a ser consultada da sua localização no array (string)
            array                           - Required: array com todos os valores do dataframe (array)

         # RETURN
             index                          - Required: retorna a posição da string consultada no array (int)
    """

    for index, value in enumerate(array):
        if string == value:
            return index

def valor_nulo(valor):
    """
        CASO VENHA UM VALOR NULO EM FORMATO DE STRING ELE RETORNA O EM VALOR INT 0

        # ARGUMENT
            valor                           - Required: valor a ser convertido (string)

        # RETURN
                                            - Required: valor convertido (int)
    """
    try:
        if valor == '':
            return 0
        else:
            return float(valor)

    except Exception as ex:
        print(ex)
        print('valor_nulo')

def callback(url):
    """
        RECEBE UMA URL EM FORMATO DE STRING E TRANSFORMA EM HIPERTEXTO

        # ARGUMENT
            url                             - Required: string com a url (string)
    """
    webbrowser.open_new(url)

def eua_to_brazil(number):
    """
        TRANSFORMA UM NUMERO NO FORMATO BRASILEIRO EM FORMATO INTERNACIONAL

        # ARGUMENT
            number                          - Required: número a ser convertido em formato internacional (float)

        # RETURN
            number                          - Required: número convertido formato internacional (float)
    """
    try:
        if ',' in number:
            change = number.replace(",",".")
            return float(change)
        else:
            return float(number)

    except Exception as ex:
        print(ex)
        print('eua_to_brazil')

def gsheets_to_dataframe(credencial, id_gsheet, aba):
    """
        ACESSA A PLANILHA DO GOOGLE SHEET E TRANSFORMA A EM UM DATAFRAME

        # ARGUMENTS
            credencial                          - Required: credencial do seu repositório no google (path)
            id_gsheet                           - Required: id da planilha (string)

        # RETURN
            dataframe                           - Required: dataframe da planilha (dataframe)
            wks                                 - Required: work sheet da planilha (work sheet)
    """
    try:
        gc = pygsheets.authorize(service_account_file=credencial)
        sh = gc.open_by_key(id_gsheet)
        wks = sh.worksheet('title', aba)

        # EXTRAÇÃO DOS PIPELINE DISPONÍVEIS
        dataframe = pd.DataFrame(wks.get_all_values())
        dataframe.rename(columns=dataframe.iloc[0, :], inplace=True)
        dataframe.drop(index=[0], inplace=True)
        return dataframe, wks

    except Exception as ex:
        print(ex)

def to_dict(arquivo):
    """
        TRANSFORMA ARQUIVOS CSV EM DICTS
        LEMBRANDO QUE É UM DEPARA BEM SACANA PRA NÃO PRECISAR USAR O JOIN

        # ARGUMENTS
            arquivo                         - Required: path do arquivo em csv (string)

        # RETURN
            dict                        - Required: retorna um dicionario depara (dict)
    """

    try:
        if isinstance(arquivo, pd.DataFrame):
            dict = {arquivo.loc[i][0]: arquivo.loc[i][1] for i in range(len(arquivo))}
        else:
            excel = pd.read_excel(f'{arquivo}')
            dict = {excel.loc[i][0]: excel.loc[i][1] for i in range(len(excel))}

    except Exception as ex:
        print(ex)
        print('to_dict')

    return dict


def split_conta(df, colunas):
    """
        CASO TENHA UMA FATURA COM MAIS DE UMA CONTA ELE SEPARA DIVIDINDO PROPORCIONALMENTE O VALOR ENTRE AS TRÊS
        CONTAS.

        # ARGUMENTS
            df                                  - Required: dataframe a ser verificado (dataframe)
            colunas                             - Required: colunas do dataframe (list)

        # RETURN
            df                                  - Required: dataframe com as contas separadas (dataframe)
    """
    try:
        for i in range(len(df)):
            if '+' in df['TIPO_CONTA'][i]:
                df_append = []
                sep = df['TIPO_CONTA'][i].split('+')
                if len(sep) == 3:
                    valor = [df['VALOR'][i] * 0.7, df['VALOR'][i] * 0.2, df['VALOR'][i] * 0.1]
                elif len(sep) == 2:
                    valor = [df['VALOR'][i] * 0.8, df['VALOR'][i] * 0.2]

                for j in range(len(sep)):
                    n_linha = [df['ID'][i],df['ID_Apart'][i], sep[j], df['DATA_VENCIMENTO'][i], df['FORMA_PAGAMENTO'][i],
                               valor[j], df['STATUS'][i]]

                    df_append.append(n_linha)
                df.drop(index=i, inplace=True)
                df_append = pd.DataFrame(df_append, columns=colunas)
                df = pd.concat([df, df_append])

        return df

    except Exception as ex:
        print(ex)
        print('split_conta')

def valoresVazios(valor):
    if isinstance(valor, int):
        valor = str(valor)
    elif isinstance(valor,str):
        if valor == '':
            valor = '0'
        else:
            valor = str(valor)
    return valor
