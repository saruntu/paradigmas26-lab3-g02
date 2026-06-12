import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder().appName("Lab3").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatMap {
      case Some(subscription) =>
        Some(subscription)

      case None =>
        println("Warning: Skipping malformed subscription")
        None
    }
    val subscriptionsRDD: RDD[Subscription] = sc.parallelize(subscriptions)

    val feedsSuccess = sc.longAccumulator("feeds exitosos")
    val feedsFailed = sc.longAccumulator("feeds fallidos")

    val postsDownloaded = sc.longAccumulator("posts descargados")
    val postsDiscarded = sc.longAccumulator("posts descartados")
    

    // Download feeds and parse posts, tracking success/failure
    val downloadResults: RDD[Post] = subscriptionsRDD.flatMap{subscription =>
      
      try{
        val feedOpt = FileIO.downloadFeed(subscription.url)
        if(feedOpt.isDefined){
          feedsSuccess.add(1)
          val posts = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name))
          postsDownloaded.add(posts.size)
          posts.iterator
        }
        else{
          feedsFailed.add(1)
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          Iterator.empty
        }
      }
      catch{
        case _: Exception =>
          feedsFailed.add(1)
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          Iterator.empty
      }
    }
    // Solo se conecta una vez a la red y queda en memoria RAM para reutilizar la info
    downloadResults.cache()
    val filteredPostsRDD: RDD[Post] = downloadResults.filter { post =>
  					post.title.nonEmpty &&
  					post.selftext.nonEmpty &&
  					post.selftext.trim.nonEmpty
				}
    
    val startIsEmpty = System.currentTimeMillis()
    
    if (downloadResults.isEmpty()){
      println("Error: No valid subscriptions found")

      //libero cache
      downloadResults.unpersist()
      sc.stop()
      return
    }
    
    val endIsEmpty = System.currentTimeMillis()
    println(s"Validation time: ${(endIsEmpty - startIsEmpty)/1000.0} seconds")

    // Flatten all posts and count JSON parse failures
    val startResults = System.currentTimeMillis()
    
    val downloadResultsList: List[Post] = downloadResults.collect().toList

    val endResults = System.currentTimeMillis()
    println(s"Download time and recollection: ${(endResults - startResults)/1000.0} seconds")
    
    // val allPosts = downloadResults.flatMap(_._2)
    val postsSuccess = downloadResultsList.length
    val postsFailed = feedsFailed.value

    // Filter empty posts
    val filteredPosts: List[Post] = filteredPostsRDD.collect().toList
    val postsFiltered = downloadResultsList.length - filteredPosts.length
    postsDiscarded.add(postsFiltered)
    
    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (filteredPosts.nonEmpty) totalChars / filteredPosts.length else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess.value.toInt,
      "feedsFailed" -> feedsFailed.value.toInt,
      "postsSuccess" -> postsDownloaded.value.toInt,
      "postsFailed" -> postsFailed.toInt,
      "postsFiltered" -> postsDiscarded.value.toInt,
      "avgChars" -> avgChars
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    val entitiesDirFile = new java.io.File(cmdArgs.entitiesDir)

    if (!entitiesDirFile.exists() || !entitiesDirFile.isDirectory) {
      println(s"Error: entities directory '${cmdArgs.entitiesDir}' not found")
      downloadResults.unpersist()
      sc.stop()
      return
    }

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)// Cargo el diccionario en el driver
    val dicBroadcast = sc.broadcast(dictionary) //le mando el diccionario una vez a cada worker para q puedan usarlo

    val entityCountsRDD: RDD[((String, String), Int)] = filteredPostsRDD.flatMap{post => //Agarro el RDD[Post] y, por c/u, devuelvo >=0 entidades
      val combinedText = s"${post.title} ${post.selftext}"
      Analyzer.detectEntities(combinedText, dicBroadcast.value)
    }
    .map{entity => //convierto cada entidad en un par clave/valor
      ((entity.entityType, entity.text), 1)
    }
    .reduceByKey(_ + _) //agrupo por clave y sumo los valores

    val startEntities = System.currentTimeMillis()
    val rankedEntities = entityCountsRDD.collect().toList //traigo el resultado final al driver
      .sortBy{case((entityType, entityText), count) => (-count, entityType, entityText)} //ordeno por cantidad descendente
    val endEntities = System.currentTimeMillis()
    println(s"Entities counting time: ${(endEntities - startEntities) / 1000.0} seconds")

    val entityCountsMap = rankedEntities.toMap
    println(Formatters.formatEntityStats(entityCountsMap, cmdArgs.topK))

    dicBroadcast.destroy() //libero el broadcast de memoria

    //libero cache
    downloadResults.unpersist()
  }
}
