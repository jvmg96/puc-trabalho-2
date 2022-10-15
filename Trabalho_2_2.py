import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from pyparsing import col

URL = "/tmp/tabela_unica.csv"

default_args = {
    'owner': "JVMG",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 15)
}

@dag(default_args=default_args, schedule='@once', catchup=False, tags=['Titanic'])
def trabalho2_2():

    @task
    def ingestao(URL):
        nome_do_arquivo = "/tmp/tabela_leitura"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(nome_do_arquivo, index=False, sep=";")
        return nome_do_arquivo

    @task
    def ind_medias(nome_do_arquivo):
        NOME_TABELA = "/tmp/resultados.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        column_names = ['PassengerId_Media','Fare_Media','SibSp+Parch_Media']
        df[column_names] = df[['PassengerId','Fare','SibSp+Parch']]
        res = df[column_names].mean()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    #Orquestração
    inicio = ingestao(URL)
    indicador = ind_medias(inicio)
    fim = EmptyOperator(task_id="fim")

    inicio >> indicador >> fim

execucao = trabalho2_2()