# Databricks notebook source
from pyspark.sql.functions import*
from pyspark.sql import SparkSession as Session
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import requests
from requests.exceptions import ChunkedEncodingError
import warnings
from pyspark import SparkFiles
from pyspark.sql import SQLContext
import json
from pyspark.sql.types import StringType, StructType, StructField
warnings.simplefilter("ignore")
import os
import re


class Cheaper_Challenge:

    def __init__(self, session=Session, df2 = py.dataframe.DataFrame, df3 = py.dataframe.DataFrame):
      #inicializando variables
      self.session = session.builder.appName("Cheaper_Challenge").getOrCreate()
      self.df2 = df2
      self.df3 = df3

    def download_data_from_bucket(self):
      
      """
       funcion que guarda los datos en formato json en la ruta preestablecida
       dentro del cluster de Databricks community
      
      """
      try:
        
        #leyendo cabecera
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
        }

        #leyendo los archivos que contienen la url
        url_files = self.session.read.format("json").option("header", "true")\
        .option("inferSchema", "true")\
        .load("/FileStore/tables/datos/urls.json")

        #obteniendo la data
        files = ["business.json", "checkin.json", "tip.json"]
        payload = []
        for size in range(len(url_files.columns)):
          for index in url_files.rdd.collect():
            r = requests.get(url=index[size], headers=headers, verify=False, stream=True)
            body = r.content
            payload.append(body.decode("utf-8"))

       #escribimos los datos en la ruta
        for index,item in enumerate(zip(payload,files)):
          dbutils.fs.put("/FileStore/tables/datos/"+files[index], payload[index], True)
          
      except ChunkedEncodingError:
        print("No se puedo establecer conexion con el servidor")
        
        
        
    def data_transformations(self):
     
      
      """
      Funcion que realiza las transformaciones de limpieza al json.
      
      """
      try:
        
        #cargamos los datos del json
        dataLakePath = "/FileStore/tables/datos/Business"
        df = self.session.read.format("json").load("/FileStore/tables/datos/business.json")

        #procesamos las columnas para iniciar la limpieza de la data
        string_cols = []
        for cols in df.columns:
          string_cols.append(cols)
          if  isinstance(df.schema[cols].dataType, StringType):
            df = (df.withColumn(cols, regexp_replace(cols,":+",","))\
                 .withColumn(cols, regexp_replace(cols,".$","")))
            self.df2 = df
            self.df3 = df

        #procesamos las columnas con tipo de datos structtype (de tipo json) para iniciar la limpieza
        string_cols  = tuple(string_cols)
        structfields = []
        schema_col =[]
        for cols in self.df2.columns:
          if isinstance(self.df2.schema[cols].dataType, StructType):
            schema_col.append(self.df2.schema[cols].dataType)
            subfields = self.df2.schema[cols].dataType.fieldNames()
            for subcols in subfields:
              structfields.append(subcols)
              self.df2 = self.df2.withColumn(f"{cols}" +"."+f"{subcols}", regexp_replace(col(f"{cols}" +"."+f"{subcols}"), ":0+", ":00"))


        #eliminando las columnas 
        string_cols = tuple(string_cols)
        self.df2 = self.df2.drop(*string_cols)

        #renombramos las nuevas columnas creadas 
        for index,item in enumerate(zip(self.df2.columns,structfields)):
          self.df2 = self.df2.withColumnRenamed(self.df2.columns[index],structfields[index])

        #iniciamos la segunda limpieza
        patterns = ["^unone","^'","'$","^[{]u","u'+"]
        replacements = ["none","","","{",""]
        for column in self.df2.columns:
          for index,item in enumerate(zip(patterns,replacements)):
            self.df2 = self.df2.withColumn(column, regexp_replace(col(column), patterns[index],replacements[index]))


        #convertimos las columnas limpiadas a una sola columna en formato json
        self.df2 = self.df2.withColumn('new_attributes', to_json(struct([col(item) for item in self.df2.columns]), options={"ignoreNullFields":False}))

        #segunda limpieza - eliminamos las columnas 
        new_cols =[]
        for columns in self.df2.columns:
          new_cols.append(columns)

        new_cols = new_cols[:-8]
        new_cols = tuple(new_cols)
        self.df2 = self.df2.drop(*new_cols)

        #convertimos las columnas limpiadas en formato json
        self.df2 = self.df2.withColumn("new_col_attributes", from_json(col("new_attributes"), schema_col[0] , options={"ignoreNullFields":False}))
        self.df2 = self.df2.withColumn('new_hours', to_json(struct([col(item) for item in self.df2.columns[:7]]), options={"ignoreNullFields":False}))

        hours_schema_col =[]
        for cols in self.df3.columns:
          if isinstance(self.df3.schema["hours"].dataType, StructType):
            hours_schema_col.append(self.df3.schema["hours"].dataType)   
        self.df2 = self.df2.withColumn("new_hour_schedule", from_json(col("new_hours"), hours_schema_col[0] , options={"ignoreNullFields":False}))

        string_type_cols = []
        for cols in self.df2.columns:
           if isinstance(self.df2.schema[cols].dataType, StringType):
              string_type_cols.append(cols)

        string_type_cols = tuple(string_type_cols)
        self.df2 = self.df2.drop(*string_type_cols)

        #realizamos el join de los dos dataframes
        # se realiza el left join por que hay multiples valores null en la columna hours
        #nota: se puede realizar un inner join para eliminar los datos nulos, se tomo el left para no depreciar la data y poder consultar sobre todo
        self.df2 = self.df2.join(self.df3, self.df3.hours == self.df2.new_hour_schedule, "left")
        self.df2 = self.df2.drop("attributes","hours")


        #guardamos el archivo en la ruta
        self.df2.repartition(1).write.format("json").mode("overwrite").save(dataLakePath)
        for file in dbutils.fs.ls("/FileStore/tables/datos/Business"):
          if file.name.endswith("json"):
            dbutils.fs.mv(dataLakePath +"/"+ file.name, dataLakePath +"/"+ "Business_cleandata.json") 
          if file.name.startswith("_"):
             dbutils.fs.rm(dataLakePath +"/"+ file.name) 

        return self.df2
      
      except java.io.FileNotFoundException:
        print("Archivo no encontrado")

   
    
    def data_discovery(self):
     
      """
      Funcion para visualizar varios insights
      """
      try:
    
        #info general
        #consultamos algunos insights preliminares 
        df1 = (self.df2.withColumn("stars", (expr("stars >=5"))).orderBy("review_count", ascending=False))
        df1.show()
        df2 = (self.df2.select("City").where(col("stars")>=5).groupBy("City").agg(count("City").alias("MaxCount")).orderBy("MaxCount",ascending=False))
        df2.show()
        df3 =(self.df2.select("Categories").where(col("stars")>=5).groupBy("Categories").agg(count("Categories").alias("MaxCountCategories"))\
              .orderBy("MaxCountCategories",ascending=False))
        df3.show()


        #descubriendo mas insights utilizando SQL Context
        #instanciamos SQlContext
        sparksqlcontext = SQLContext(self.session)
        #creamos la vista
        self.df2.createOrReplaceTempView("Business")

        #buscamos mas insights
        #negocios con wifi gratis  y sin registros vacios(null)
        df4 = sparksqlcontext.sql("""SELECT new_col_attributes, new_hour_schedule ,address, business_id, categories , city ,name,
        review_count , stars, state FROM Business where new_col_attributes.wifi like 'free' and business_id is not null order by stars desc""")
        df4.show()

        #negocios que aceptan tarjetas de creditos y que es recreativo para niños sin registros vacios(null)
        df5 = sparksqlcontext.sql("""SELECT new_col_attributes, new_hour_schedule ,address, business_id, categories , city ,name,
        review_count , stars, state FROM Business where new_col_attributes.BusinessAcceptsCreditCards=='True'
        and new_col_attributes.GoodForKids = 'True' and business_id is not null 
        group by new_col_attributes, new_hour_schedule ,address, business_id, categories , city ,name,
        review_count , stars, state
        order by stars desc""")
        df5.show()

        #cargamos los datos del archivo
        dataLakePath = "/FileStore/tables/datos/tip.json"
        df6 = self.session.read.format("json").load(dataLakePath)
        #creamos una segunda vista
        df6.createOrReplaceTempView("tips")

        #Join de la tabla tips con Business (para esta operacion no se encontraron datos)
        df6 = sparksqlcontext.sql("""SELECT new_col_attributes, new_hour_schedule ,address, Business.business_id as business_data_id , categories , city ,name,
        review_count , stars, state, tips.business_id as tip_business_id, tips.text FROM Business inner join tips on
        tips.business_id == Business.business_id where tips.text is not null""")
        df6.show()


        #negocios abierto lunes y viernes empezando a las 8:30 y terminando a las 22:30 sin registros vacios de business id
        df7 = sparksqlcontext.sql("""SELECT new_col_attributes, new_hour_schedule ,address, business_id  , categories , city ,name,
        review_count , stars, state FROM Business where new_hour_schedule.Monday like '%8:30%' and  new_hour_schedule.Friday like '%22:30%' 
        and business_id is not null """)
        df7.show()


        #guardamos el archivo en la ruta (df1)
        dataLakePath = "/FileStore/tables/datos/df1"
        df1.repartition(1).write.format("json").mode("overwrite").save(dataLakePath)
        for file in dbutils.fs.ls("/FileStore/tables/datos/df1"):
          if file.name.endswith("json"):
            dbutils.fs.mv(dataLakePath +"/"+ file.name, dataLakePath +"/"+ "df1.json") 
          if file.name.startswith("_"):
             dbutils.fs.rm(dataLakePath +"/"+ file.name) 
        
      except java.io.FileNotFoundException:
        print("Archivo no encontrado")
      
          
    def upload_data_to_bucket(self):
      
      """
        funcion para realizar la carga al bucket de los jsons para la prueba
        si se va a cargar desde un string(cadena de caracteres ej: json, lista, etc) se utiliza el metodo:
        upload_from_string
        si se va a cargar desde un archivo(se debe generar la ruta) se utiliza el metodo:
        upload_from_filename
      """
      try:
        
        #cargamos las credenciales para cargar la info al bucket
        dbutils.fs.cp("/FileStore/tables/datos/key.json", "file:/tmp/key.json")
        
        dbutils.fs.cp("/FileStore/tables/datos/Business/Business_cleandata.json", "file:/tmp/Business_cleandata.json")
        path = '/tmp/Business_cleandata.json'
        with open('/tmp/key.json', "r") as json_file:
            key_value = json.load(json_file)

        #leemos los archivos
        with open('/tmp/Business_cleandata.json', "r") as json_data:
          data = json_data.read()
          data = data.replace("{}","false")
          data = json.dumps(data)
          data = json.loads(data)
          
        dbutils.fs.cp("/FileStore/tables/datos/df1/df1.json", "file:/tmp/df1.json")  
        with open('/tmp/df1.json', "r") as df1_data:
          df1 =  df1_data.read()
          df1 = json.dumps(df1)
          df1 = json.loads(df1)


        ##cargamos los archivos al bucket de google storage
        #asignamos las credenciales
        credentials = service_account.Credentials.from_service_account_info(key_value)
        client = storage.Client(credentials=credentials)
        bucket = client.get_bucket("bucketrroaprueba")
        blob = bucket.blob("Business_data.json")
        df1_blob = bucket.blob("df1.json")
        blob.upload_from_string(data)
        df1_blob.upload_from_string(df1)
        
      except (google.cloud.exceptions.NotFound, java.io.FileNotFoundException):
        print("error")




if __name__ == "__main__":
    data_transform = Cheaper_Challenge()
    data_transform.download_data_from_bucket()
    data_transform.data_transformations()
    data_transform.data_discovery()
    data_transform.upload_data_to_bucket()
  
