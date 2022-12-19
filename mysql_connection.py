#Importação da biblioteca Pandas para iniciar o processo de ETL, para tratamento dos arquivos CSV disponibilizados 
import pandas as pd

# Executamos o comando read_csv para a leitura dos arquivos e organizando-os em tabelas
transactions = pd.read_csv ('C:\EstrelaBet\\transactions.csv', index_col=False, encoding='utf-8', delimiter = ';')
users = pd.read_csv ('C:\EstrelaBet\\users.csv', index_col=False, encoding='utf-8', delimiter = ';')

# Verificação dos dados e tabelas criadas pelo comando read_csv
transactions.head()
users.head()
users.dtypes
transactions.dtypes

# Importei o módulo 'locale' para definir a configuração numérica padrão do usuário do Brasil (PT-BR UTF-8).
# Os dados das colunas transactions['value'] foi identificado como int64 e users['affiliate_id'] como float64. Assim, fez-se necessário 
# a utilização do comando locale.atof e users.astype('Int64') para convertê-los em float64 e int64, respectivamente. Situação semelhante ocorreu 
# com as colunas de data, sendo assim excutei o comando pd.to_datetime para correta leitura dos dados.  
import locale
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

# Iniciando o processo de conexão entre o Python e o database MySQL
# Importação da biblioteca mysql.connector junto ao módulo Error para se conectar ao database e identificar possíveis erros no 
# processo de inserção de dados.
import mysql.connector as mysql
from mysql.connector import Error

# Comando mysql.connector.connect para testar a conexão do Python ao MySQL Server 
cnx = mysql.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="")

# Get a cursor
cur = cnx.cursor()

# Executa uma consulta teste
cur.execute("SELECT CURDATE()")

# Busca um resultado teste
row = cur.fetchone()
print("Current date is: {0}".format(row[0]))

# Fecha a conexão
cnx.close()

# Abre conexão ao MySQL;
# Criação do database;
try:
    conn = mysql.connect(host='127.0.0.1', user='root', password='')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE estrela_bet")
        print("Database is created")
except Error as e:
    print("Error while connecting to MySQL", e)

# Seleciona o database criado anteriormente
# Exclui tabelas pré-existentes denominadas users e/ou transactions;
# Cria as tabelas users e transactions com as definições apropriadas de colunas e chave primária;
# Insere os dados, linha a linha através de um loop, nas respectivas colunas e commita o código;
# Caso ocorram erros no processo, o módulo Error identificará e apresentará através da função print() a descrição do error.
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