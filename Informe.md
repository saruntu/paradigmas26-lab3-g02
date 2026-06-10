# EJERCICIO 1
## a) Dibujar un diagrama de flujo de los pasos que realiza el programa como un grafo de dependencias. Explicitar el tipo en Scala de cada conexión.


### Pipeline original (sin Spark)

![Pipeline original](data/flujo_prev.png)

### Pipeline distribuido con Spark
El programa tiene un pipeline casi secuencial, el driver inicializa SparK, carga los datos y luego distribuye el trabajo entre los workers.

![Pipeline Spark](data/flujo_next.png)
  
## b) Para cada caso del pipeline, determinar si puede expresarse como unade las abstracciones de Spark. (map, flatMap, reduceByKey) 

La descarga y el parseo de feeds puede expresarse con flatMap, ya que cada subscription puede traer una cantidad variable de posts, varios si la descarga es exitosa o ninguno si ocurre un error.

La extraccion de las entidades tambien lo hace con flatMap, porque cada post puede tener cero, una o varias entidades.

La clasificacion de las entidades puede hacerse con map, ya que cada una se transforma en un resultado tipo ((entityType, text), 1)

El agrupar y llevar un conteo de cuantas veces aparece una entidad puede usar reduceByKey, agrupando con la misma clave (entityType, text) y sumando las ocurrencias.

El ranking de las identidades usariamos una operacion de ordenamiento como sortBy y esta es la que no encaja con map, flatMap ni reduceByKey ya que requiere comprar los resultados ibtenidos y ordenarlos segun su ocurrencia.

Por otro lado, la lectura del archivo de subscription, la creacion de la sesion de Spark y la impresion de los resultados tampoco encajan con las abstracciones ya que son tareas que realizar el driver y no transformaciones distribuidas sobre un RDD.

## c) Identificar que pasos del pipeline son barreras y cuales pueden ejecutarse de forma completamente independientes entre workers.

La etapa de descargas de feed, extraer las entidades y la clasificacion pueden ejecutarse de forma completamente independiente entre workers porque cada worker procesa elementos distintos sin necesidad de saber los resultados de los
otros.

El conteo con reduceByKey constituye una barrera de sincronizacion, porque es necesario agrupar y combinar las ocurrencias de una misma entidad proveniente de distintos workers antes de dar el resultado final.

La etapa de ranking tambien requiere una coordinacion global entre workers, ya que para ordenar las entidades es necesario disponer los conteos completos de la etapa anterior, por ende solo puede ejecutarse una vez finalizada los conteos anteriores.

## d)El mecanismo de extensión (extension point) de Spark es la función que el desarrollador le pasa a cada transformación. ¿Qué restricciones impone Spark sobre esas funciones para que puedan ejecutarse en un entorno distribuido? Piensen en serialización, estado compartido y efectos secundarios.

Las funciones utilizadas en transformaciones de Spark deben ser serializables, ya que Spark las envia desde el driver hacia los workers para su ejecucion distribuida. Ademas no deben depender de estado compartido mutable ya que cada worker ejecuta su copia independiente de la funcion y no existe memoria compartida entre ellos. 


