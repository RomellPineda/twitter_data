package twitter_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.DataFrameReader
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import scala.concurrent.Future

object Runner {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
      .appName("Hello Spark SQL")
      .master("local[4]")
      .getOrCreate()

              //we want to always add an import here, it enables some syntax and code generation:
              // if you run into mysterious errors with what should be working code, check to make sure this import exists
              import spark.implicits._

              spark.sparkContext.setLogLevel("WARN")

              //helloSparkSql(spark)
              //helloTweetStream(spark)

              helloMySQL(spark)

    spark.stop()
  }
  
  def helloMySQL(spark: SparkSession): Unit = {
  
      import spark.implicits._
    
      val schemaPeople = spark.read.json("twitterstream")
      schemaPeople.createOrReplaceTempView("PeopleTable") // create database
    
      val teenagers = spark.sql("SELECT data.text FROM PeopleTable")
      val results = teenagers.collect()
          results.foreach(println)
   
  }

  def helloTweetStream(spark: SparkSession): Unit = {
    import spark.implicits._
    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetStreamToDir(bearerToken, queryString = "?tweet.fields=geo&expansions=geo.place_id")
    }

    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000) {
      filesFoundInDir = Files.list(Paths.get("twitterstream")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if(!filesFoundInDir) {
      println("Error: Unable to populate tweetstream after 30 seconds.  Exiting..")
      System.exit(1)
    }
  }

  def tweetStreamToDir(
      bearerToken: String,
      dirname: String = "twitterstream",
      linesPerFile: Int = 1000,
      queryString: String = ""
  ) = {
    val httpClient = HttpClients.custom
      .setDefaultRequestConfig(
        RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
      )
      .build()
    val uriBuilder: URIBuilder = new URIBuilder(
      s"https://api.twitter.com/2/tweets/sample/stream$queryString"
    )
    val httpGet = new HttpGet(uriBuilder.build())
    //set up the authorization for this request, using our bearer token
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()
    if (null != entity) {
      val reader = new BufferedReader(
        new InputStreamReader(entity.getContent())
      )
      var line = reader.readLine()
      //initial filewriter, replaced every linesPerFile
      var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
      var lineNumber = 1
      val millis = System.currentTimeMillis()
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          Files.move(
            Paths.get("tweetstream.tmp"),
            Paths.get(s"$dirname/tweetstream-$millis-${lineNumber/linesPerFile}"))
          fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        }
        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }
    }
  }
}
