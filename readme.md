<h1 align="center"> Spark Bulk Upsert</h1>

![Develope Badge](https://img.shields.io/badge/pyspark-3.5.3-blue)
![Develope Badge](https://img.shields.io/badge/psycopg2-2.9.9-blue)
![Scope Badge](https://img.shields.io/badge/upsert-red)

A python library to facilitate upsert operations on all kinds of databases using a bulking approach for better performance. It leverages ```Apache Spark``` for data extraction, manipulation and comparison, while using ```psycopg2``` for the upsert process on the database.


<h2 align="left">Example of use</h2>

```python
connection_properties = {'db_name':database_dw, 'user':user_dw, 'password':password_dw, 'host':host_dw, 'port':port_dw, 'driver': driver}

spark = SparkSession\
    .builder\
    .appName("Extraction_Data")\
    .config("spark.driver.extraClassPath", path_jdbc)\
    .getOrCreate()

sql_new_eixos = 'select distinct id as nk_eixos, descricao from stg_projeto_investimento_eixos'
sql_old_eixos = 'select nk_eixos, descricao from dim_eixos de'
table_name_eixos = 'public.dim_eixos'
key_columns_eixos = ['nk_eixos']
bulk_size = 500
crud_database_table(spark, sql_old_eixos, sql_new_eixos, table_name_eixos, key_columns_eixos, connection_properties, bulk_size)
```
