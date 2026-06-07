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
    val subscriptions = subscriptionOpts.flatten
    val subscriptionsRDD: RDD[Subscription] = sc.parallelize(subscriptions)

    val feedsSuccess = sc.longAccumulator("feeds exitosos")
    val feedsFailed = sc.longAccumulator("feeds fallidos")

    // Download feeds and parse posts, tracking success/failure
    val downloadResults: RDD[Post] = subscriptionsRDD.flatMap{subscription =>
      
      try{
        val feedOpt = FileIO.downloadFeed(subscription.url)
        if(feedOpt.isDefined){
          feedsSuccess.add(1)
          val posts = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name))
          posts.iterator
        }
        else{
          feedsFailed.add(1)
          println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
          Iterator.empty
        }
      }
      catch{
        case _: Exception => 
          println(s"Warning: Failed to download from'${subscription.name}' (${subscription.url})")
          Iterator.empty
      }
    }

    if (downloadResults.isEmpty()){
      println("Error: No valid subscriptions found")
      sc.stop()
      sys.exit(1)
    }

    // val downloadResults = subscriptions.map { subscription =>
    //   val feedOpt = FileIO.downloadFeed(subscription.url)
    //   val posts = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name))
    //   (feedOpt.isDefined, posts)
    // }

    // Count feed successes/failures
    // val feedsSuccess = downloadResults.count(_._1)
    // val feedsFailed = downloadResults.length - feedsSuccess

    // Flatten all posts and count JSON parse failures
    val downloadResultsList: List[Post] = downloadResults.collect().toList

    // val allPosts = downloadResults.flatMap(_._2)
    val postsSuccess = downloadResultsList.length
    val postsFailed = feedsFailed.value

    // Filter empty posts
    val filteredPosts = Analyzer.filterEmptyPosts(downloadResultsList)
    val postsFiltered = downloadResultsList.length - filteredPosts.length

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (filteredPosts.nonEmpty) totalChars / filteredPosts.length else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess.value.toInt,
      "feedsFailed" -> feedsFailed.value.toInt,
      "postsSuccess" -> postsSuccess,
      "postsFailed" -> postsFailed.toInt,
      "postsFiltered" -> postsFiltered,
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

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
  }
}
