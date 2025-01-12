import mysql.connector
import pandas as pd
from database import DATABASE_CONFIG

def conectar_banco():
    return mysql.connector.connect(
        host=DATABASE_CONFIG["host"],
        user=DATABASE_CONFIG["user"],
        password=DATABASE_CONFIG["password"],
        database=DATABASE_CONFIG["database"]
    )

def definir_prioridade(pacientes):
    def calcular_prioridade(row):
        prioridade = 0

        if row['idade'] >= 60:
            prioridade += 3
        if 'febre alta' in row['sintomas'].lower():
            prioridade += 3
        if 'sinusite' in row['sintomas'].lower():
            prioridade += 1
        if row['frequencia_cardiaca'] and (row['frequencia_cardiaca'] < 60 or row['frequencia_cardiaca'] > 100):
            prioridade += 4
        if 'falta de ar' in row['sintomas'].lower() and 'dor no peito' in row['sintomas'].lower():
            prioridade += 5
        if 'tontura' in row['sintomas'].lower():
            prioridade += 3
        if 'desmaio' in row['sintomas'].lower():
            prioridade += 4

        if prioridade >= 10:
            return 'Alta'
        elif prioridade >= 4:
            return 'Media'
        else:
            return 'Baixa'

    pacientes['prioridade'] = pacientes.apply(calcular_prioridade, axis=1)
    return pacientes

def atualizar_prioridade_no_banco(pacientes, conn):
    cursor = conn.cursor()
    for _, row in pacientes.iterrows():
        query = """
        UPDATE pacientes
        SET prioridade = %s
        WHERE id = %s
        """
        cursor.execute(query, (row['prioridade'], row['id']))
    conn.commit()

def processar_fila():
    conn = conectar_banco()
    query = "SELECT * FROM pacientes"
    pacientes = pd.read_sql(query, conn)
    pacientes = definir_prioridade(pacientes)
    atualizar_prioridade_no_banco(pacientes, conn)
    conn.close()

def carregar_pacientes():
    conn = conectar_banco()
    query = "SELECT * FROM pacientes"
    pacientes = pd.read_sql(query, conn)
    conn.close()
    return pacientes

if __name__ == "__main__":
    processar_fila()
    pacientes = carregar_pacientes()
    print(pacientes.head())
