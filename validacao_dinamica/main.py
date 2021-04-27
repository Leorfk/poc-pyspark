from services.validacao_dinamica import *
from pyspark.sql import SparkSession

path = r'/media/leo/leorfk/Projetos/python/validacao_dinamica/jsons/regras.json'
regra_evento = pega_evento(path)
chaves = pega_chaves(regra_evento)
regras = pega_regras(regra_evento)
condicao = monta_validacao(regras)
print(condicao)#vamos precisar analisar a profundidade do json

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value")\
    .getOrCreate()
# spark is from the previous example.
sc = spark.sparkContext

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files
path = r'/media/leo/leorfk/Projetos/python/validacao_dinamica/jsons/evento.json'
capturaDF = spark.read.json(path)

# The inferred schema can be visualized using the printSchema() method
capturaDF.printSchema()
# root
#  |-- age: long (nullable = true)
#  |-- name: string (nullable = true)

# Creates a temporary view using the DataFrame
capturaDF.createOrReplaceTempView("evento_mv")

# SQL statements can be run by using the sql methods provided by spark
query = f'SELECT codigo_produto FROM evento_mv WHERE {condicao}'
print(query)
eventos_LFC_DF = spark.sql(query)
eventos_LFC_DF.show()
# +------+
# |  name|
# +------+
# |Justin|
# +------+

# Alternatively, a DataFrame can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string
# jsonStrings = ['{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}']
# otherPeopleRDD = sc.parallelize(jsonStrings)
# otherPeople = spark.read.json(otherPeopleRDD)
# otherPeople.show()
# +---------------+----+
# |        address|name|
# +---------------+----+
# |[Columbus,Ohio]| Yin|
# +---------------+----+