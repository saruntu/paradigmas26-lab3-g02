import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.io.FileNotFoundException

object FileIO {

  /**
   * Read subscriptions from JSON file.
   * @param filePath path to subscriptions file
   * @return list of options: Some(Subscription) for valid entries, None for malformed entries
   *         returns empty list if file not found
   */
  def readSubscriptions(filePath: String): List[Option[Subscription]] = {
    implicit val formats: Formats = DefaultFormats
    try{
      val source = Source.fromFile(filePath)
      val content = source.mkString
      source.close()

      val json = parse(content)
      val subscriptions = json.extract[List[Map[String, String]]]

      subscriptions.map { sub =>
        if(sub("name").nonEmpty && sub("url").nonEmpty){
          Some(Subscription(sub("name"), sub("url")))
        }
        else{
          None
        }
      }
    }
    catch{
      case _: FileNotFoundException => 
        println(s"Error: Could not load $filePath - file not found")
        Nil
      case err: Exception =>
        println(s"Error: Could not load $filePath - invalid JSON format")
        Nil  
    }
    // val source = Source.fromFile(filePath)
    // val content = source.mkString
    // source.close()

    // val json = parse(content)
    // val subscriptions = json.extract[List[Map[String, String]]]

    // subscriptions.map { sub =>
    //   Some(Subscription(sub("name"), sub("url")))
    // }
  }

  /**
   * Download feed JSON from URL.
   * @param url Reddit feed URL
   * @return Option containing JSON as String, None on network error or timeout
   */
  def downloadFeed(url: String): Option[String] = {
    try{
      val source = Source.fromURL(url)
      val content = source.mkString
      source.close()
      Some(content)
    }
    catch{
      case _: Exception => println(s"Warning: Network error or malformed URL for $url")
      None
    }
    // val source = Source.fromURL(url)
    // val content = source.mkString
    // source.close()
    // Some(content)4
  }

  /**
   * Read dictionary file line by line.
   * @param filePath path to dictionary file
   * @return Option containing list of entities, None if file missing
   */
  def readDictionaryFile(filePath: String): Option[List[String]] = {
    try{
      val source = Source.fromFile(filePath)
      try{
        val lines = source.getLines()
          .map(_.trim)
          .filter(_.nonEmpty)
          .filterNot(_.startsWith("#"))
          .toList
        Some(lines)
      }
      catch{
        case _: Exception=> None
      }
      finally{
        source.close()
      }
    }
    catch{
      case _: FileNotFoundException =>
        println(s"Warning: Could not load $filePath")
        None
    }
    
    // val source = Source.fromFile(filePath)
    // val lines = source.getLines()
    //   .map(_.trim)
    //   .filter(_.nonEmpty)
    //   .filterNot(_.startsWith("#"))
    //   .toList
    // source.close()
    // Some(lines)
  }
}
