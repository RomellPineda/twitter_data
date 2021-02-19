package twitter_data

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrame
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet
import org.apache.http.util.EntityUtils
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import scala.concurrent.Future
import java.io.FileNotFoundException
import java.io.IOException
import java.time._
import java.lang.Thread

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("twitter_data")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    var positiveHashTags = Array("love", "congratulations", "thank you", "exciting", "excited", "favorite", "fav", "amazing", "lovely", "incredible", "elated", "😀", "😃", "😄", "😁", "😆", "😚", "😘", "🥰", "😍", "🤩", "🥳", "🙂", "☺️", "😊", "😏", "😋", "😎", "❤️", "♥", "👍", "🙌");
    var negativeHashTags = Array("hate", "stop", "angry", "stupid", "horrible", "worst", "sucks", "bad", "disappointing", "😐", "😰", "😰", "😔", "☹️", "🙁", "😕", "😟", "🥺", "😢", "😥", "😓", "😞", "😖", "😣", "😩", "😫", "🤢", "🤮", "💔", "🖕");
    var runnerindex = 0
    
    while(runnerindex < 10){
      //populate tweetstream.tmp and users.tmp simultaneously
      helloTweetStream(spark)

      //join tweetstream.tmp and users.tmp into a DF
      val joinedDataFrame = joinTweetAndUsersTemp(spark)
      joinedDataFrame.show()
      joinedDataFrame.printSchema()

      //add analysis functions here
      //positive tweets
      analize_hashtags(spark, positiveHashTags, joinedDataFrame); 

      //roml function calls
      //romlfunction() -returns an array of results - 10 negative 20 postive, 9 negative 5 postive...
      //1) query
      //2) produce results
      //3) writes the output to a file or to command line with each loop
      //example return = [10,20] *next loop* [9,5] 
      //^after 2 loops, you would have something like [19,25]
      //array += romlfunction()

      //negative tweets
      analize_hashtags(spark, negativeHashTags, joinedDataFrame);

      //these functions should take in joinedDataFrame, perform spark.sql() analysis, and then return a DF
      //we can either print intermediate results for loop, or we can keep track of results throughout all loops
      //or we can combine all of our tables throughout each loop and perform analysis on those after

      /*
      add functionality to append our data to s3
      */

      //implement running index
      runnerindex += 1
      //print loop # to console
      println(s"*** END OF TWEET STREAM LOOP #"+runnerindex+" ***")
      
      //write tweetstream.tmp to long-term file (Ronald)
    }
  }

  def helloTweetStream(spark: SparkSession): Unit = {
    import spark.implicits._
    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))
    //streamed data - tweets
    val tweetstreamURI = "https://api.twitter.com/2/tweets/search/stream"
    //non-streamed data - users
    val userURI = "https://api.twitter.com/2/users"
    //temp user ID string prior to function
    var tempuserstring = ""
      
    //try catch error handling-----------------------
    /*
    waits 20 minutes when no files avail from tweet stream 
    in reality, we can pull 50 times from online v2 tweet stream every 15 min,
    but 20 mins is a safe call
    */
    try {
      //get tweet stream to .tmp and read from it
      tweetStreamToDir(bearerToken, uriString = tweetstreamURI + "?tweet.fields=created_at&expansions=author_id")
      tempuserstring = usersStringFromTweetStream(spark)
    } catch {
      case e: org.apache.spark.sql.AnalysisException => {      
        println("Tweet call limit reached! Waiting 20 minutes...")
        Thread.sleep(1200000)
        //get tweet stream to .tmp and read from it
        tweetStreamToDir(bearerToken, uriString = tweetstreamURI + "?tweet.fields=created_at&expansions=author_id")
        tempuserstring = usersStringFromTweetStream(spark)
      }
    }
    //end of error handling-----------------------

    //add user IDs to end of get-users URL
    val testUserIdString = "?ids=" + tempuserstring
    tweetStreamToDir(bearerToken, dirname = "users", uriString = userURI + testUserIdString + "&user.fields=created_at,public_metrics,verified")
  }

  def tweetStreamToDir(
      bearerToken: String,
      dirname: String = "twitterstream",
      linesPerFile: Int = 1000,
      uriString: String = ""
  ) = {

    // AWS configs
    // val bucketName = "usf-210104-big-data"
    // val key = System.getenv(("DAS_KEY_ID"))
    // val secret = System.getenv(("DAS_SEC"))

    // AWS client set up
    // val awsCredentials = new BasicAWSCredentials(key, secret)
    // val amazonS3Client = new AmazonS3Client(awsCredentials)

    val httpClient = HttpClients.custom
      .setDefaultRequestConfig(
        RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
      )
      .build()
    val uriBuilder: URIBuilder = new URIBuilder(
      uriString
    )
    //println(uriString)
    val httpGet = new HttpGet(uriBuilder.build())
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()

    if (null != entity) {
      if (dirname == "users") {
        val userData2 = EntityUtils.toString(entity, "UTF-8")

        //parse user JSON - DO NOT TOUCH :O
        //----------------------------
        var userData1 = userData2.replace("""{"data":[""", "")
        var userData1a = userData1.replace("]}", "")
        //start of file changes every time, so use temp string below to deliminate lines for json
        var tempstring = userData1.charAt(0).toString +userData1.charAt(1).toString +userData1.charAt(2).toString +userData1.charAt(3).toString +userData1.charAt(4).toString +userData1.charAt(5).toString
        //println(tempstring)
        var userData1b = userData1a.replace(tempstring, "\n" + tempstring)
        var userData = userData1b + "}"
        //----------------------------
        //create filewriter
        var fileWriter = new PrintWriter(Paths.get("users.tmp").toFile)
        //write JSON string to users.tmp
        fileWriter.write(userData)
        fileWriter.flush()
        fileWriter.close()

        // Write user data to s3
        // val millis = System.currentTimeMillis()
        // val folderName = s"$dirname/data-$millis"
        // var fileToUpload = new File("users.tmp")
        // amazonS3Client.putObject(bucketName, folderName, fileToUpload)
      } else {
        val reader = new BufferedReader(
          new InputStreamReader(entity.getContent())
        )
        var line = reader.readLine()
        var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        var lineNumber = 1
        val millis = System.currentTimeMillis()
        //create stream index
        var index = 1
        //while (line != null) {
        //this while condition gets about 70-90 tweets!!!
        while (index < 100) {
          if (lineNumber % linesPerFile == 0) {
            fileWriter.close()
            // AWS file path
            // val fileName = s"$dirname/tweetstream-$millis-${lineNumber/linesPerFile}"
            Files.move(
              Paths.get("tweetstream.tmp"),
              Paths.get(s"$dirname/tweetstream-$millis-${lineNumber/linesPerFile}"))
              // Configured to write data to s3 and local
              // Paths.get(fileName))
            fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
            // Write to AWS
            // var fileToUpload = new File(fileName)
            // amazonS3Client.putObject(bucketName, fileName, fileToUpload)

          }
  
          fileWriter.println(line)
          line = reader.readLine()
          lineNumber += 1
          //index to 100 will stop stream
          index += 1
          //fileWriter.close()
        }
      }
    }
  }

  

    def usersStringFromTweetStream(spark: SparkSession): String ={
      /*
        generates and returns a string of user IDs to be fed to the get-users URL
        -NECESSARY parsing of these user IDs takes place in this function, and in tweetStreamToDir()
      */
      
      //last tweet entry in tweet.tmp is always corrupted, so the next two lines remove that line
      import sys.process._
      Seq("sed","-i","$ d","tweetstream.tmp")!

      import spark.implicits._

      //get author IDs from tweetstream.tmp
      //-----------------------------------------
      // this is where i mess stuff up!!!
      //read in stream of tweets into a temp view
        val tweettempDF = spark.read.json("tweetstream.tmp")
        tweettempDF.createOrReplaceTempView("tweetstemp")
        val tweettempQueryDF = spark.sql("SELECT data.author_id from tweetstemp")
      //-----------------------------------------

      //turn the DF from above (user/author IDs) into a string of userIDs
      //-----------------------------------------
      //create list of author IDs from the view
      var list_author_id = tweettempQueryDF.select("author_id").collect().map(_(0)).toList
      //convert list to string
      import scala.collection.mutable.ListBuffer
      var string_author_id_pre = list_author_id.toString
      //----------------------------------------

      //ADDITIONAL PARSING (performed before the parsing block in tweetStreamToDir())
      //----------------------------
      var string_author_id_pre1 = string_author_id_pre.replace("List(", "")
      var string_author_id_pre2 = string_author_id_pre1.replace(" ", "")
      var string_author_id = string_author_id_pre2.replace(")", "")
      //println(string_author_id)
      //----------------------------

      //return string of author IDs (user IDs) from tweets
      //note: THIS RESULT RECEIVES ADDITIONAL PARSING in tweetStreamToDir()
      return string_author_id
  }

  def joinTweetAndUsersTemp(spark: SparkSession): DataFrame = {
      /*
      creates DFs of tweets and users (up to 100 lines each) and
      joins the two DFs into one workable DF for our analysis

      NOTE: observe tweetstream.tmp and users.tmp to get a grasp of the schema,
      and how to reference it thru spark.sql()
      */
    
      //get tweet DF for tweetstream.tmp
      //-------------------------------------
      val tweetDF = spark.read.json("tweetstream.tmp")
      tweetDF.createOrReplaceTempView("tweets")
      //test tweet DF
      //tweetDF.show()
      //tweetDF.printSchema()
      //-------------------------------------

      //get user DF from users.tmp
      //-------------------------------------
      val userDF = spark.read.json("users.tmp")
      userDF.createOrReplaceTempView("users")
      //test user DF
      //userDF.show()
      //userDF.printSchema()
      //-------------------------------------

      //join users and tweets DFs together
      //-------------------------------------
      val joinedDF = spark.sql("select * from tweets inner join users on users.id = tweets.data.author_id")
      //show resulting joined DF
      //joinedDF.show()
      //-------------------------------------

      //return the joined DF
      return joinedDF
  }

  
 def analize_hashtags(spark: SparkSession, arr: Array[String], joinedDF: DataFrame): Unit = {
   /*
   analyzes pos/neg hashtag string
   */
   var twtSQL = "";
   joinedDF.createOrReplaceTempView("TweetTable") // create database 
   // COUNT THE HASHTAGS --------------
   println("Analyzing the Data --------")
   for (i <- 0 to arr.length - 1) {
       twtSQL = "SELECT Count(*) AS Count FROM TweetTable WHERE lower(data.text) LIKE '%" + arr(i) + "%'";
       val twtCount = spark.sql( twtSQL );
       println( arr(i) + " : " + twtCount.head().get(0) );
   }
  }
}