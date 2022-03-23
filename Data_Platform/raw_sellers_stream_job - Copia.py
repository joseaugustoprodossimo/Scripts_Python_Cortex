import argparse
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, MapType

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--environment', help='current environment running (dev/prod)', default='dev', choices=['dev', 'prod'])
    arg_parser.add_argument('--source', help='current seller', default='MAGAZINELUIZA')
    arg_parser.add_argument('--date_process', help='date -1', default=(datetime.today() - timedelta(days=1)).date())

    args = arg_parser.parse_args()
    environment = args.environment
    source = args.source
    date_process = str(args.date_process)

    spark = SparkSession \
        .builder \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .appName("sellers") \
        .getOrCreate()

    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info(f'Current environment: {environment}')

    # Apenas para uso local
    # input_path = f'/Users/eduardo.lepri/Downloads/MAGALU/landing/{source}/date={date_process}'
    # output_path = '/Users/eduardo.lepri/Downloads/MAGALU/sellers/raw'

    date_obj = datetime.strptime(date_process, '%Y-%m-%d')
    day = date_obj.strftime('%d')
    month = date_obj.strftime('%m')
    year = date_obj.strftime('%Y')

    # Apenas para uso local
    # input_path = f'/Users/eduardo.lepri/workspace/cortex/s3/sellers/stream/sellers/landing/{source}/year={year}/month={month}/day={day}'
    # output_path = f'/Users/eduardo.lepri/workspace/cortex/s3/sellers/stream/sellers/raw/'

    input_path = f's3a://cortex-data-platform-landing-area-{environment}/predictus/stream/sellers/{source}/year={year}/month={month}/day={day}'
    output_path = f's3a://cortex-data-platform-raw-area-{environment}/raw_sellers_{environment}/sellers/vendedores/'

    sellers_schema = [
        # Sellers Fields
        StructField("id", StringType(), False),
        StructField("url", StringType(), True),
        StructField("fonte", StringType(), True),
        StructField("titulo", StringType(), True),
        StructField("valor", StringType(), True),
        StructField("pagamento", StringType(), True),
        StructField("dataProcessamento", StringType(), True),
        StructField('cycle', StringType(), True),

        StructField('vendedor', StructType([
            StructField('id', StringType(), True),
            StructField('cnpj', StringType(), True),
            StructField('url', StringType(), True),
            StructField('nome', StringType(), True),
            StructField('razaoSocial', StringType(), True),
            StructField('endereco', StringType(), True),
            StructField('regiao', StringType(), True),
            StructField('cep', StringType(), True),
            StructField('quantidadeTotalProdutos', StringType(), True),
            StructField('categoriaPrincipal', StringType(), True),
            StructField('totalAvaliacoes', StringType(), True),
            StructField('quantidadeTotalProdutosPrime', StringType(), True),

            StructField("categorias", MapType(StringType(), StringType())),
            StructField("avaliacoes", MapType(StringType(), StringType())),
            StructField("marcas", MapType(StringType(), StringType())),
            StructField("tipoProdutos", MapType(StringType(), StringType())),
            StructField("condicaoProdutos", MapType(StringType(), StringType())),
            StructField("faixaDesconto", MapType(StringType(), StringType())),
            StructField("faixaPreco", MapType(StringType(), StringType())),

        ])),

        StructField('_metadata', StructType([
            StructField('source', StringType(), True),
            StructField('cycle', StringType(), True),
        ])),

    ]

    schema = StructType(sellers_schema)
    sellers_columns = [i.name for i in sellers_schema]

    # read input
    # df = spark.read.json(input_path, schema=schema)
    df = spark.read.schema(schema).json(input_path)

    df = df.withColumn('fonte', df._metadata.source)
    df = df.withColumn('cycle', df._metadata.cycle)
    df = df.withColumn("extraction_date", F.to_date(F.lit(date_process), format='yyyy-MM-dd'))

    df = df.select(*sellers_columns, 'extraction_date')

    # apenas para usos local
    # df.limit(20).write.format('hive').parquet(output_path, mode='overwrite', partitionBy=['fonte', 'extraction_date'])
    # df.repartition('fonte', 'extraction_date').write.parquet(output_path, mode="overwrite", partitionBy=['fonte', 'extraction_date'])

    spark.catalog.setCurrentDatabase(f'raw_sellers_{environment}')
    df.coalesce(1000).write.format('parquet').option('path', output_path.replace('s3a', 's3')).saveAsTable('vendedores', mode='overwrite', partitionBy=['fonte', 'extraction_date'])

    spark.sparkContext._jvm.System.gc()

spark.stop()