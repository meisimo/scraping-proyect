# Scrapy Digesto
### Descripción

Script que va a la web http://digesto.asamblea.gob.ni/consultas/coleccion, trae la información y y la guarda en una base de datos mongo.

### Estructura de la BD

Principalmente la se establecerá la base de datos con el nombre de `digesto`. En esta base se intentá tener una estructura similar a las carpetas de la página fuente. Esto es,una colección por carpeta con el nombre en _snake case_.

#### Filtro de datos

Cada dato contará con un mínimo de requisitos para poder ser guardado en la base de datos. Estos requisitos son:

* Tener la fecha en formato _dd/mm/yyyy_
* Tener un título

### Requerimientos de sistema

* python2.7
* pip
* base de datos mongo

### Pasos para preparar el script

1. En el directorio del proyecto ejecutar el comando
```bash
    pip install -r Requirements.txt 
```
2. Iniciar el servidor _mongo_ si no lo está.
3. Dentro del archivo _src/main2_7.py_ editar el constante **MONGO_URI** con la dirección en del servidor de la base de datos


### Ejecutar el script

1. Desde el directorio del proyecto ejecutar el comando
```bash
    scrapy runspider src/main2_7.py -s DOWNLOAD_DELAY=0.25 --nolog
```
