## Cheaper Challenge

###### Transformaciones con Spark - Databricks

## Requisitos Previos

* 1 -> Tener cuenta o crear una cuenta en Databricks, enlace: https://community.cloud.databricks.com/login.html

* 2 -> usar la version 2.1 del script que se encuentra en el repositorio.

## Ejecución del Script

* 1 -> para ejecutar el script se debe crear un nuevo notebook en databricks con el archivo fuente que se encuentra en este repo 

* 2 -> para el funcionamiento correcto del script deben instalar las siguientes librerias:

     * 1. google-cloud-storage
     * 2. google-auth
     * 3. requests

* 3 -> crear una carpeta datos dentro del sistema de archivos de databricks, debe tener la siguiente ruta **_/FileStore/tables/datos_**

* 4 -> Al crear la carpeta se debe subir el archivo **_urls.json_** a la siguiente ruta **_/FileStore/tables/datos/_**

* 5 -> se debe crear un archivo **_key.json_** con las credenciales para subir la informacion a los buckets, este archivo json se genera directamente en google y tambien  se  debe copiar en la ruta **_/FileStore/tables/datos/_**, no comparto el mio por seguridad.

     ## Nota
     * una vez que el cluster quede deshabilitado en la version community se debe crear otro cluster con las librerias mencionadas anteriormente
     * Es importante tener la key de autenticación de google cloud storage asociada la cuenta para subir y bajar archivos del bucket 
     * Es necesario configurar el usuario en IAM de GCP con todos los permisos de accesos a las reglas de cloud storage y los buckets, sino generara error


## Recomendaciones

* Tener una cuenta en GCP y tener habilitados los permisos de lectura , escritura para google cloud storage(Buckets)


###### enjoy!
