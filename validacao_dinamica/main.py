from services.validacao_dinamica import cria_condicao
from pyspark.sql import SparkSession

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