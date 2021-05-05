import json
from pyspark.sql import SparkSession

def pega_evento(path):
    arquivo_regras = open(path)
    evento = json.load(arquivo_regras)
    return evento


def pega_chaves(file_item):
    chaves = []
    for chave in file_item:
        chaves.append(chave)
    return chaves


def pega_regras(arquivo):
    for chave in arquivo:
        if type(arquivo[chave]) is list:
            return arquivo[chave]


def monta_validacao(regras):
    condicao = ''
    for regra in regras:
        for chave, valor in regra.items():
            if chave == 'valor_campo':
                condicao += str(f"'{valor}'")
            else:
                condicao += str(valor)
    return condicao


def cria_condicao(path):
    return monta_validacao(pega_regras(pega_evento(path)))

def poc_select():
    path = r'/media/leo/leorfk/Projetos/python/validacao_dinamica/jsons/regras.json'
    path_event = r'/media/leo/leorfk/Projetos/python/validacao_dinamica/jsons/evento.json'
    viewName = 'evento_mv'
    condicao = cria_condicao(path)
    spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value")\
        .getOrCreate()
    sc = spark.sparkContext
    capturaDF = spark.read.json(path_event)
    capturaDF.printSchema()
    capturaDF.createOrReplaceTempView(viewName)
    query = f'''SELECT
    codigo_produto,
    sigla_sistema,
    valor_transacao_financeira,
    origem.sigla_sistema_origem,
    origem.numero_agencia_origem,
    origem.numero_conta_origem,
    origem.indireto.codigo_ispb_liquidante,
    origem.indireto.numero_agencia_indireto,
    origem.indireto.numero_conta_indireto,
    destino.sigla_sistema_origem,
    destino.numero_agencia_origem,
    destino.numero_conta_origem
    FROM {viewName} WHERE {condicao}'''
    print(query)
    eventos_LFC_DF = spark.sql(query)
    eventos_LFC_DF.show()