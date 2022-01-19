import sqlite3

def conecta_banco(endereco):
    """
        CONECTA AO BANCO SQLITE GERANDO A CONEXÃO

        # ARGUMENTS
            endereço                            - Required: caminho do db (string)

        # RETURN
            conn                                - Required: conexão com o db (sqlite file)
    """

    try:
        conn = sqlite3.connect(endereco)
        return conn

    except Exception as ex:
        print(ex)
        print('conecta_banco')

def update_values(conn, sql, valores):
    """
        UTILIZA A FUNCAO UPDATE DO SQL NA CONEXÃO INDICADA

        # ARGUMENTS
            conn                            - Required: conexão com o banco de dados (sqlite file)
            sql                             - Required: query de update - no lugar de valores trocar por ? (string)
            valores                         - Required: valores a serem inseridos (tuple)
    """
    cursor = conn.cursor()
    cursor.execute(sql, valores)
    conn.commit()

def consulta_db(endereco, sql):
    """
        CONSULTA DO TIPO SELECT A UM BANCO SQLITE3

        # ARGUMENTS
            endereco                            - Required: path do banco de dados (string)
            sql                                 - Required: query do select (string)

        # RETURN
            consulta                            - Required: resultado da consulta (list of list)
    """

    # ESTABELECENDO CONEXAO
    db = sqlite3.connect(endereco)

    # ABRINDO O BANCO
    cursor = db.cursor()

    # EXECUTANDO A CONSULTA
    cursor.execute(sql)
    consulta = cursor.fetchall()

    # FECHA A CONEXAO COM O BANCO
    cursor.close()

    return consulta

def input_values(endereco,tabela,campos,valores):
    """
        INPUT DE DADOS DENTRO DO BANCO DE DADOS

        # ARGUMENTS
            endereco                            - Required: path do db (string)
            tabela                              - Required: nome da tabela (string)
            campos                              - Required: nome das colunas (tuple)
            valores                             - Required: valores a serem inputados (list)
    """

    # ESTABELECENDO CONEXAO
    db = sqlite3.connect(endereco)

    # ABRINDO O BANCO
    cursor = db.cursor()

    # CONTAGEM DE CAMPOS
    n = len(campos)
    iteracao = n*'?,'
    iteracao = iteracao[:-1]


    # EXECUTANDO O INSERT
    sql = """INSERT INTO {} {} VALUES ({})""".format(tabela, tuple(campos), iteracao)
    cursor.execute(sql, valores)
    db.commit()

    # FECHA A CONEXAO COM O BANCO
    cursor.close()