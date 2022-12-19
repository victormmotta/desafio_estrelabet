#Importação da biblioteca Pandas para iniciar o processo de ETL, para tratamento dos arquivos CSV disponibilizados 
import os
import json
import pandas as pd
import locale
import mysql.connector as mysql
from mysql.connector import Error

def read_csv():
    # Executamos o comando read_csv para a leitura dos arquivos e organizando-os em tabelas
    global users
    global transactions
    transactions = pd.read_csv ('C:\\Users\\Meu PC\\codingestrelabet\\desafio_estrelabet\\arquivos_utilizados\\transactions.csv', index_col=False, encoding='utf-8', delimiter = ';')
    users = pd.read_csv ('C:\\Users\\Meu PC\\codingestrelabet\\desafio_estrelabet\\arquivos_utilizados\\users.csv', index_col=False, encoding='utf-8', delimiter = ';')

    # Verificação dos dados e tabelas criadas pelo comando read_csv
    transactions.head()
    users.head()
    users.dtypes
    transactions.dtypes

    # Importei o módulo 'locale' para definir a configuração numérica padrão do usuário do Brasil (PT-BR UTF-8).
    # Os dados das colunas transactions['value'] foi identificado como int64 e users['affiliate_id'] como float64. Assim, fez-se necessário 
    # a utilização do comando locale.atof e users.astype('Int64') para convertê-los em float64 e int64, respectivamente. Situação semelhante ocorreu 
    # com as colunas de data, sendo assim excutei o comando pd.to_datetime para correta leitura dos dados.  
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    transactions['value'] = transactions.value.apply(locale.atof)
    users['affiliate_id'] = users['affiliate_id'].fillna(0)
    users['affiliate_id'] = users['affiliate_id'].astype('Int64')
    transactions['date'] = pd.to_datetime(transactions['date']).dt.date
    users['register_date'] = pd.to_datetime(users['register_date']).dt.date

    # Fiz uma nova verificação nas tabelas para garantir o tratamento realizado nos dados
    transactions.head()
    users.head()
    users.dtypes
    transactions.dtypes

    # Executei o comando to_sql para ajustar os registros do DataFrame para um database SQL 
    transactions.to_sql
    users.to_sql

def mysql_conn():
    # Abre conexão ao MySQL;
    # Criação do database; 
    try:
        conn = mysql.connect(host='127.0.0.1', user='root', password='')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS estrela_bet")
            print("Database is created")
    except Error as e:
        print("Error while connecting to MySQL", e)

# Seleciona o database criado anteriormente
# Exclui tabelas pré-existentes denominadas users e/ou transactions;
# Cria as tabelas users e transactions com as definições apropriadas de colunas e chave primária;
# Insere os dados, linha a linha através de um loop, nas respectivas colunas e commita o código;
# Caso ocorram erros no processo, o módulo Error identificará e apresentará através da função print() a descrição do error.

def mysql_populate():
    try:
        conn = mysql.connect(host='127.0.0.1', database='estrela_bet', user='root', password='')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            cursor.execute('DROP TABLE IF EXISTS users;')
            cursor.execute('DROP TABLE IF EXISTS transactions;')
            print('Creating table....')
            cursor.execute("CREATE TABLE users (user_id INT NOT NULL AUTO_INCREMENT,affiliate_id INT, register_date DATE,  PRIMARY KEY (user_id))")
            print("Table is created....")
            cursor.execute("CREATE TABLE transactions (user_id INT NOT NULL,date DATE,value FLOAT(53,0),transaction_type VARCHAR(100))")
            print("Table is created....")
            for i,row in users.iterrows(): 
                sql = "INSERT INTO estrela_bet.users VALUES (%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                print("Record inserted")
                conn.commit()
            for i,row in transactions.iterrows(): 
                sql = "INSERT INTO estrela_bet.transactions(user_id, date, value, transaction_type) VALUES (%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                print("Record inserted")
                conn.commit()
    except Error as e:
                print("Error while connecting to MySQL", e)

if __name__ == '__main__':
    try:
        # Connect to MySQL and perform initial testing
        mysql_conn()
    except Error as e:
        print("Error connecting to MySQL.", e)

    try:
        # Connect to MySQL and perform initial testing
        read_csv()
        mysql_populate()
    except Error as e:
        print("Error processing MySQL or CSV.", e)