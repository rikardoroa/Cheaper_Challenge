# Databricks notebook source
from pyspark.sql.functions import*
from pyspark.sql import SparkSession as Session
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import requests
from requests.exceptions import ChunkedEncodingError
import warnings
import pyspark.sql as py
import json
from pyspark.sql.types import StringType, StructType, StructField
warnings.simplefilter("ignore")
import re
from datetime import datetime

class Cheaper_Challenge:

    def __init__(self, session=Session, df2 = py.dataframe.DataFrame, df3 = py.dataframe.DataFrame, key = dict, my_bucket = storage.bucket.Bucket,  
                 client = storage.client.Client):
      #inicializando variables
      self.session = session.builder.appName("Cheaper_Challenge").getOrCreate()
      self.df2 = df2
      self.df3 = df3
      self.key = key
      self.my_bucket = my_bucket
      self.client = client

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

        #asignamos las columnas con los datos limpios al dataframe
        hours_schema_col =[]
        for cols in self.df3.columns:
          if isinstance(self.df3.schema["hours"].dataType, StructType):
            hours_schema_col.append(self.df3.schema["hours"].dataType)   
        self.df2 = self.df2.withColumn("new_hour_schedule", from_json(col("new_hours"), hours_schema_col[0] , options={"ignoreNullFields":False}))

        #eliminamos las copias de las columnas 
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
        
        #negocios con cinco estrellas ordenador por review_count
        df1 = (self.df2.withColumn("stars", (expr("stars >=5"))).orderBy("review_count", ascending=False))
        df1.show()
        
        #total ciudades cuyos negocios obtuvieron una calificacion de cinco estrellas, agrupado todo por ciudad
        df2 = (self.df2.select("City").where(col("stars")>=5).groupBy("City").agg(count("City").alias("MaxCount")).orderBy("MaxCount",ascending=False))
        df2.show()
        
        #total de categorias cuyos negocios obtuvieron una calificacion de cinco estrellas, agrupado todo por categoria , ordenado por categoria descendente
        df3 =(self.df2.select("Categories").where(col("stars")>=5).groupBy("Categories").agg(count("Categories").alias("MaxCountCategories"))\
              .orderBy("MaxCountCategories",ascending=False))
        df3.show()

        
        #todos los negocios con wifi libre y sin valores nulos en la columna Business_id ordenados por calificacion(estrellas) de manera descendente
        df2 = (self.df2.select("new_col_attributes", "new_hour_schedule" ,"address", "business_id", "categories" , "city","name",
                      "review_count" , "stars", "state")\
        .where(col("new_col_attributes.wifi") == 'free').filter(col("Business_id").isNotNull())\
        .orderBy("stars",ascending=False))
        df2.show()
      
        #total de ciudades con negocios que aceptan tarjetas de credito y con ambiente adecuado para ni??os sin valores nulos en el Business_id
        #agrupados por ciudades ordenado de manera descendente
        df3 = (self.df2.select("new_col_attributes", "new_hour_schedule" ,"address", "business_id", "categories" , "city" ,"name",
                        "review_count" , "stars", "state")\
        .where((col("new_col_attributes.BusinessAcceptsCreditCards") =='True')  & (col("new_col_attributes.GoodForKids") == 'True'))\
        .filter(col("Business_id").isNotNull()).groupBy("city").agg(count("city").alias("Total_cities")).orderBy("Total_cities",ascending=False))

        df3.show()
        
        
        #cargamos los datos del archivo
        dataLakePath = "/FileStore/tables/datos/tip.json"
        df6 = self.session.read.format("json").load(dataLakePath)
        
        #join con la tabla tips y business
        #todos los negocios con review (campo text_review de la tabla tips)
        df4 = self.df2.withColumnRenamed("business_id", "b_id")
        df6 = df6.withColumnRenamed("text", "text_review")
        df4 = df4.join(df6, df6.business_id == df4.b_id, "inner")\
        .select("new_col_attributes", "new_hour_schedule", "address", "b_id" , "categories" , "city" ,"name",
                "review_count" , "stars", "state", "business_id", "text_review").filter(col("text_review").isNotNull())
        df4.show()
        
        
        #todos los negocios que abren el lunes a las 8:30 am y cierra el viernes a las 22:30 pm donde no hay valores nulos en el Business_id
        df5 = self.df2.select("new_col_attributes", "new_hour_schedule" ,"address", "business_id", "categories" , "city" ,"name",
                    "review_count" , "stars", "state")\
        .filter((col("new_hour_schedule.Monday").like("%8:30%"))  & (col("new_hour_schedule.Friday").like("%22:30%")) &
            (col("Business_id").isNotNull()))

        df5.show()

       
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
        
    #obteniendo las credenciales para subir la data al bucket
    def get_credentials(self):
      """
      Funcion que asigna las credenciales de la cuenta
      """
      
      try:
        
        dbutils.fs.cp("/FileStore/tables/datos/key.json", "file:/tmp/key.json")
        with open('/tmp/key.json', "r") as json_file:
          key_value = json.load(json_file)
          self.key = key_value

        return self.key
      
      except java.io.FileNotFoundException:
        print("Archivo no encontrado")
        
        
    
    def validate_bucket(self):
      """
      Funcion para cargar el bucket
      """
      
      try:
        
        #cargamos las credenciales de la cuenta para validar el bucket
        bucket_name = "bucketrroaprueba"
        credentials = service_account.Credentials.from_service_account_info(self.key)
        client = storage.Client(credentials=credentials)
        buckets = client.list_buckets()
        
        #validamos que el bucket exista  y si no existe se crea
        for bucket in buckets:
            if bucket_name in bucket.name:
                my_bucket = client.get_bucket(bucket_name)   
            else:
                bucket = client.bucket(bucket_name)
                my_bucket = client.create_bucket(bucket, location="us")
            
            self.my_bucket = my_bucket
            self.client = client
            return self.client
          
      except Exception as e:
        print(e)
          
        
        
    def upload_files(self):
      """
      Funcion para subir los archivos al bucket
      """
      
      try:
        #cargamos la ruta de los archivos y la clave de autenticacion
        now = datetime.now()
        dt_string = now.strftime("%Y%d%H%M%S")
      
        dbutils.fs.cp("/FileStore/tables/datos/Business/Business_cleandata.json", "file:/tmp/Business_cleandata.json")
        dbutils.fs.cp("/FileStore/tables/datos/df1/df1.json", "file:/tmp/df1.json")
        name_file = ["Business_cleandata","df1"]
        local_file = ["/tmp/Business_cleandata.json","/tmp/df1.json"]
        
        #instanciamos el bucket donde se va a subir toda la data
        my_bucket = self.client.get_bucket(self.my_bucket)
        
        
        #subimos los archivos desde la ruta y los cargamos al bucket
        for index,item  in enumerate(zip(local_file,name_file)):
          with open(local_file[index], "r") as json_data:
            data = json_data.read()
            compile_pattern = re.compile(r'{}')
            pattern_found = bool(re.search(compile_pattern,data ))
            
            if pattern_found is True:
              data = data.replace("{}","false")
              data = json.dumps(data)
              data = json.loads(data)
              blob = my_bucket.blob(name_file[index] +  dt_string + ".json")
              blob.upload_from_string(data)
              blob.make_public()
              url = blob.public_url
                                        
            if pattern_found is False:
              data = json.dumps(data)
              data = json.loads(data)
              blob = my_bucket.blob(name_file[index] +  dt_string + ".json")
              blob.upload_from_string(data)
              blob.make_public()
              url = blob.public_url
            
        
      except (NotFound):
        print("error")

    

if __name__ == "__main__":
    data_transform = Cheaper_Challenge()
    data_transform.download_data_from_bucket()
    data_transform.data_transformations()
    data_transform.data_discovery()
    data_transform.get_credentials()
    data_transform.validate_bucket()
    data_transform.upload_files()
  
