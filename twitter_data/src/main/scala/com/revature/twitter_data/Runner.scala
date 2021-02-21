package twitter_data

import scala.collection.mutable.ArrayBuffer
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
import scala.io.Source

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("twitter_data")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    //positive and negative hashtags
    var positiveHashTags = Array("love", "congratulations", "thank you", "exciting", "excited", "favorite", "fav", "amazing", "lovely", "incredible", "elated", "😀", "😃", "😄", "😁", "😆", "😚", "😘", "🥰", "😍", "🤩", "🥳", "🙂", "☺️", "😊", "😏", "😋", "😎", "❤️", "♥", "👍", "🙌");
    var negativeHashTags = Array("hate", "stop", "angry", "stupid", "horrible", "worst", "sucks", "bad", "disappointing", "😐", "😰", "😰", "😔", "☹️", "🙁", "😕", "😟", "🥺", "😢", "😥", "😓", "😞", "😖", "😣", "😩", "😫", "🤢", "🤮", "💔", "🖕");
    //index for running the loop
    var runnerindex = 1
    //lists for ronalds functions
    var positive_arr = Array(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
    var negative_arr = Array(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
    //q5 counter
    var q5pos = 0
    var q5neg = 0
    //q6 counter
    var q6pos = ArrayBuffer[Double]()
    var q6neg = ArrayBuffer[Double]()
    //menu looper
    var looper = true

    //MENU START***************************************************************************************************************************
    do {
    //print menu
    printmenu()
    //prompt user for input
    var menu_input = scala.io.StdIn.readLine()
    if(menu_input == "1"){
      println("\n*** GROUP 6 PROJECT MISSION ***\n")
      println("To find solutions to the following broad questions:")
      println("Are tweets generally positive or negative?")
      println("What makes a tweet positive or negative in nature?\n")
      println("To our estimation, some conclusions can be made about tweet positivity/negativity by observing at the following factors:\n" +
        "-Indicators of Positive/Negative Tweets\n" +
        "-Time of day\n" +
        "-Active/Inactive Users\n" +
        "-Age of User Accounts\n" +
        "-Celebrity Tweets\n" +
        "-Total Followers\n")
    }
    else if(menu_input == "2"){
      //non-streamed analysis here----------------------------------------------------------------------------------------------------
      println("\nStarting archived tweet positivity/negativity analysis...\n")
      //q2---
      time_pos_neg(spark)
      //q3---
      Question3.run(spark)
      //----------------------------------------------------------------------------------------------------
    }
    else if(menu_input == "3"){
      println("\nStarting real-time tweet positivity/negativity analysis...\n")
      while(runnerindex <= 10000){
        //populate tweetstream.tmp and users.tmp simultaneously
        helloTweetStream(spark)

        //join tweetstream.tmp and users.tmp into a DF
        val joinedDataFrame = joinTweetAndUsersTemp(spark)

        //add analysis functions here****************************************************************************************************************************
        //q1---
        display_five_pos_neg(joinedDataFrame, spark)
        
        //ronald's functions for count of positive and negative indicators---
        positive_arr = analize_hashtags(spark, positiveHashTags, joinedDataFrame, "Positive", positive_arr);
        negative_arr = analize_hashtags(spark, negativeHashTags, joinedDataFrame, "Negative", negative_arr);

        //q4---
        Question4.run(spark)

        //q5---
        q5pos = celeb_tweets(joinedDataFrame, spark, "positive", q5pos)
        q5neg = celeb_tweets(joinedDataFrame, spark, "negative", q5neg)

        //q6---
        q6pos = followers_pos_neg(spark, joinedDataFrame, "positive", q6pos, runnerindex)
        q6neg = followers_pos_neg(spark, joinedDataFrame, "negative", q6neg, runnerindex)
        //END ANALYSIS FUNCTIONS****************************************************************************************************************************

       //estimated # tweets observed
        println(s"Estimated # of total positive/negative tweets observed through $runnerindex streamed samples:")
        println(runnerindex*80)
        //print loop # to console
        println(s"*** END OF STREAM UPDATE #"+runnerindex+" ***")
        //implement running index
        runnerindex += 1
        //wait 5 seconds after each stream loop
        Thread.sleep(5000)
      }
  }
  else if(menu_input == "4"){
    println("\nThank you for using Revature Group 6 Twitter Positivity/Negativity Analysis Application!")
    looper = false
  }
  else{
    println("\nInvalid menu input\n")
  }
  }while(looper)
}
//MENU END***************************************************************************************************************************


  def helloTweetStream(spark: SparkSession): Unit = {
    /*
    handles tweet stream to .tmp
    */
    import spark.implicits._
    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))
    //streamed data - tweets
    val tweetstreamURI = "https://api.twitter.com/2/tweets/search/stream"
    //non-streamed data - users
    val userURI = "https://api.twitter.com/2/users"
    //temp user ID string prior to function
    var tempuserstring = ""
      
    //try catch error handling-----------------------
    tempuserstring = try_catch_tweetstream(tweetstreamURI, spark)
    
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

    //AWS configs
    // val bucketName = "bigdata-pj2-mstanco"
    // val key = System.getenv(("DAS_KEY_ID"))
    // val secret = System.getenv(("DAS_SEC"))

    // //AWS client set up
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
        val millis = System.currentTimeMillis()
        val folderName = s"$dirname/data-$millis"
        var fileToUpload = new File("users.tmp")
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
        //this while condition gets about 70-90 tweets!!!
        while (index < 100) {
          if (lineNumber % linesPerFile == 0) {
            fileWriter.close()
            // AWS file path
            val fileName = s"$dirname/tweetstream-$millis-${lineNumber/linesPerFile}"
            Files.move(
              Paths.get("tweetstream.tmp"),
              //Paths.get(s"$dirname/tweetstream-$millis-${lineNumber/linesPerFile}"))
              // Configured to write data to s3 and local
              Paths.get(fileName))
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
      //-------------------------------------

      //get user DF from users.tmp
      //-------------------------------------
      val userDF = spark.read.json("users.tmp")
      userDF.createOrReplaceTempView("users")
      //-------------------------------------

      //join users and tweets DFs together
      //-------------------------------------
      val joinedDF = spark.sql("select * from tweets inner join users on users.id = tweets.data.author_id")
      //-------------------------------------

      //return the joined DF
      return joinedDF
  }

  
 def analize_hashtags(spark: SparkSession, arr: Array[String], joinedDF: DataFrame, pos_neg: String, pos_neg_list: Array[Int]): Array[Int] = {
   /*
   analyzes pos/neg hashtag string, increments count thru loop
   */
   var twtSQL = "";
   joinedDF.createOrReplaceTempView("TweetTable") // create database 
   // COUNT THE HASHTAGS --------------
   println(s"\nCount of $pos_neg Indicators in Tweet Stream Thus Far--------")
   for (i <- 0 to arr.length - 1) {
       twtSQL = "SELECT Count(*) AS Count FROM TweetTable WHERE lower(data.text) LIKE '%" + arr(i) + "%'";
       val twtCount = spark.sql( twtSQL );
       var counttotal = twtCount.head().get(0).toString.toInt + pos_neg_list(i)
       println( arr(i) + ": " + counttotal);
       //increment array of ints
       pos_neg_list(i) = pos_neg_list(i)+twtCount.head().get(0).toString.toInt
   }
   return pos_neg_list
  }

  //q2-------------------------------------------------------------------------------------------------------------------------------------------------
  def time_pos_neg(spark: SparkSession): Unit = {
      /*
      counts positive and negative tweets by hour
      */
      //create longtermDF from the dump
      val longtermDF = spark.read.json("longtermtweets_dump.txt")
      longtermDF.createOrReplaceTempView("tweets_LT")

      //positive tweets total
      val LTDFpos = spark.sql("select count(*) as total_pos_tweets from tweets_LT where matching_rules.tag[0] like '%positive%'")
      println("\nResults from 131,000 tweet sample---")
      LTDFpos.show()
      //negative tweets total
      val LTDFneg = spark.sql("select count(*) as total_neg_tweets from tweets_LT where matching_rules.tag[0] like '%negative%'")
      LTDFneg.show()

      //positive tweets by time of day
      val LTDFposhr = spark.sql("select substr(data.created_at,12,2) as hour_UTC, count(*) as count_pos from tweets_LT where matching_rules.tag[0] like '%positive%' group by data.created_at order by hour_UTC asc")
      LTDFposhr.createOrReplaceTempView("poshours")
      val LTDFposfinal  = spark.sql("select hour_UTC, sum(count_pos) as count_pos_tweets from poshours group by hour_UTC order by hour_UTC asc")
      LTDFposfinal.createOrReplaceTempView("poshoursfinal")

      //negative tweets by time of day
      val LTDFneghr = spark.sql("select substr(data.created_at,12,2) as hour_UTC, count(*) as count_neg from tweets_LT where matching_rules.tag[0] like '%negative%' group by data.created_at order by hour_UTC asc")
      LTDFneghr.createOrReplaceTempView("neghours")
      val LTDFnegfinal  = spark.sql("select hour_UTC, sum(count_neg) as count_neg_tweets from neghours group by hour_UTC order by hour_UTC asc")
      LTDFnegfinal.createOrReplaceTempView("neghoursfinal")

      //join pos and neg tweets by hour and perform aggregtions
      val joinedbyhour = spark.sql("select neghoursfinal.hour_UTC, poshoursfinal.count_pos_tweets, neghoursfinal.count_neg_tweets, sum(poshoursfinal.count_pos_tweets/(poshoursfinal.count_pos_tweets+neghoursfinal.count_neg_tweets)) as pct_pos, sum(neghoursfinal.count_neg_tweets/(poshoursfinal.count_pos_tweets+neghoursfinal.count_neg_tweets)) as pct_neg from poshoursfinal inner join neghoursfinal on neghoursfinal.hour_UTC = poshoursfinal.hour_UTC group by neghoursfinal.hour_UTC, poshoursfinal.count_pos_tweets, neghoursfinal.count_neg_tweets order by neghoursfinal.hour_UTC asc")
      println("\nResults by hour UTC from 131,000 tweet sample---")
      joinedbyhour.show()
  }

  def try_catch_tweetstream(tweetstreamURI: String, spark: SparkSession): String = {
    /*
    try catch loop that keeps running code, so that we always get streamed data
    */

    //bearer token
    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))
    var tempuserstring = ""
    try {
      //get tweet stream to .tmp and read from it
      tweetStreamToDir(bearerToken, uriString = tweetstreamURI + "?tweet.fields=created_at&expansions=author_id")
      tempuserstring = usersStringFromTweetStream(spark)
    } catch {
      case e: org.apache.spark.sql.AnalysisException => {      
        println("No tweets found. Trying again in two minutes...")
        //sleep 2 mins
        Thread.sleep(120000)
        tempuserstring = try_catch_tweetstream(tweetstreamURI, spark)
      }
    }
    return tempuserstring
  }

  //q1-------------------------------------------------------------------------------------------------------------------------------------------------
  def display_five_pos_neg(df1 : DataFrame, spark: SparkSession): Unit = {
      /*
      ANSWERS q1 BY DISPLAYING 5 POSITIVE AND NEGATIVE TWEETS
      */
    
      df1.createOrReplaceTempView("joineddf")

      //display 5 pos tweets
      val fivepos = spark.sql("select data.text as text from joineddf where matching_rules.tag[0] like '%positive%' limit 5 ")
      println("\n5 positive tweets in this set-------")
      val posstring = fivepos.select("text").collect().map(_.getString(0)).mkString("\nTWEET:")
      println(posstring)
      println("\n")

      //display 5 neg tweets
      val fiveneg = spark.sql("select data.text as text from joineddf where matching_rules.tag[0] like '%negative%' limit 5 ")
      println("\n5 negative tweets in this set-------")
      val negstring = fiveneg.select("text").collect().map(_.getString(0)).mkString("\nTWEET:")
      println("TWEET:" + negstring)
      println("\n")
  }

  //q5-------------------------------------------------------------------------------------------------------------------------------------------------
  def celeb_tweets(df5 : DataFrame, spark: SparkSession, pos_neg: String, counttotal: Int): Int = {
      /*
      ANSWERS q5 BY OBSERVING TWEETS WHERE VERIFIED = TRUE
      */
      df5.createOrReplaceTempView("joineddf")

      //count and return total of positive or negative tweets by celebrities, depending on inputs
      val pos_celeb_tweets = spark.sql(s"select count(*) from joineddf where matching_rules.tag[0] like '%$pos_neg%' and verified = 'true'")
      var counttotalfinal = pos_celeb_tweets.head().get(0).toString.toInt + counttotal
      println(s"\nCount of $pos_neg Tweets by Celebrities in Tweet Stream Thus Far--------")
      println(counttotalfinal)
      println("\n")
      return counttotalfinal
  }

  //q6-------------------------------------------------------------------------------------------------------------------------------------------------
  def followers_pos_neg(spark: SparkSession, q6DF: DataFrame, pos_neg: String, countarray: ArrayBuffer[Double], loopcount: Int): ArrayBuffer[Double]={
      /*
      counts positive and negative tweets by hour
      */

      //create longtermDF from the dump
      q6DF.createOrReplaceTempView("tweets_LT")

      //calcuate average followers per pos/neg tweet
      //then append to array
      //then avg array to get total average depending on loop #
      val avg_follow = spark.sql(s"select avg(public_metrics.followers_count) as avg_followers from tweets_LT where matching_rules.tag[0] like '%$pos_neg%'")
      countarray += avg_follow.head().get(0).toString.toDouble
      println(s"\nAverage followers by creators of $pos_neg tweets in Tweet Stream Thus Far--------")
      //println(avg_follow.head().get(0).toString.toDouble)
      var total = 0.0;      
      for ( i <- 0 to (countarray.length - 1)) {
         total += countarray(i);
         //println(countarray(i))
      }
      println(total/countarray.length)
      println("\n")
      return countarray
  }

  def printmenu(): Unit ={
    /*
    displays main menu 
    */
    println("\nREVATURE GROUP 6 TWITTER POSITIVITY/NEGATIVITY ANALYSIS APPLICATION---------------------------------------------------------")
    println("Developers: Ronald Hernandez, Romell Pineda, Michael Stanco\n")
    println("*** MAIN MENU OPTIONS ***")
    println("1. View Project Mission")
    println("2. View Analysis on Archived Data")
    println("3. View Real-Time Analysis")
    println("4. Exit")
    println("------------------------------------------------------------------------------------------------------------------------------")
  }
}