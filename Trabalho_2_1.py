import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

URL = "https://raw.githubusercontent.com/jvmg96/puc_trabalho_2/main/titanic.csv"

default_args = {
    'owner': "JVMG",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 15)
}

@dag(default_args=default_args, schedule='@once', catchup=False, tags=['Titanic'])
def trabalho2_1():

    @task
    def ingestao(URL):
        nome_do_arquivo = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(nome_do_arquivo, index=False, sep=";")
        return nome_do_arquivo

    @task
    def ind_passageiros(nome_do_arquivo):
        NOME_TABELA1 = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res1 = df.groupby(['Sex', 'Pclass']).agg({
            "PassengerId": "count"
        }).reset_index()
        print(res1)
        res1.to_csv(NOME_TABELA1, index=False, sep=";")
        return NOME_TABELA1

    @task
    def ind_tarifas(nome_do_arquivo):
        NOME_TABELA2 = "/tmp/tarifa_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res2 = df.groupby(['Sex', 'Pclass'])['Fare'].mean().reset_index()
        print(res2)
        res2.to_csv(NOME_TABELA2, index=False, sep=";")
        return NOME_TABELA2

    @task
    def ind_parentes(nome_do_arquivo):
        NOME_TABELA3 = "/tmp/parentes_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        column_names = ['SibSp','Parch']
        df['SibSp+Parch'] = df[column_names].sum(axis=1)
        res3 = df.groupby(['Sex','Pclass'])['SibSp+Parch'].sum().reset_index()
        print(res3)
        res3.to_csv(NOME_TABELA3, index=False, sep=";")
        return NOME_TABELA3

    @task
    def ind_unico(NOME_TABELA1, NOME_TABELA2, NOME_TABELA3):
        TABELA_UNICA = "/tmp/tabela_unica.csv"
        df1 = pd.read_csv(NOME_TABELA1, sep=";")
        df2 = pd.read_csv(NOME_TABELA2, sep=";")
        df3 = pd.read_csv(NOME_TABELA3, sep=";")
        res = df1.merge(df2, how='inner', on=['Sex','Pclass'])
        res = res.merge(df3, how='inner', on=['Sex','Pclass'])
        print(res)
        res.to_csv(TABELA_UNICA, index=False, sep=";")
        return TABELA_UNICA

    #Orquestração
    inicio = ingestao(URL)
    indicador1 = ind_passageiros(inicio)
    indicador2 = ind_tarifas(inicio)
    indicador3 = ind_parentes(inicio)
    indicador_unico = ind_unico(indicador1, indicador2, indicador3)
    fim = EmptyOperator(task_id="fim")
    triggerdag = TriggerDagRunOperator(
        task_id="trigga_DAG2",
        trigger_dag_id="trabalho2_2"
    )

    inicio >> indicador1 >> indicador_unico
    inicio >> indicador2 >> indicador_unico
    inicio >> indicador3 >> indicador_unico
    indicador_unico >> fim >> triggerdag

execucao = trabalho2_1()