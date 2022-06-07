## Cheaper Challenge

###### Transformaciones con Spark - Databricks

## Requisitos Previos

* 1 -> Tener cuenta o crear una cuenta en Databricks, enlace: https://community.cloud.databricks.com/login.html

## Ejecución del Script

* 1 -> para ejecutar el script se debe crear un nuevo notebook en databricks con el archivo fuente que se encuentra en este repo 

* 2 -> para el funcionamiento correcto del script deben instalar las siguientes librerias:

     * 1. google-cloud-storage
     * 2. google-auth
     * 3. requests

* 3 -> crear una carpeta datos dentro del sistema de archivos de databricks, debe tener la siguiente ruta **_/FileStore/tables/datos_**


     ## Nota
     * una vez que el cluster quede deshabilitado en la version community se debe crear otro cluster con las librerias mencionadas anteriormente
     * Es importante tener la key de autenticación de google cloud storage asociada la cuenta para subir y bajar archivos del bucket 
     * Es necesario configurar el usuario en IAM de GCP con todos los permisos de accesos a las reglas de cloud storage y los buckets, sino generara error


## Recomendaciones

* Tener una cuenta en GCP y tener habilitados los permisos de lectura , escritura para google cloud storage(Buckets)


###### enjoy!
