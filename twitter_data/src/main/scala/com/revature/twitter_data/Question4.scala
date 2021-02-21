package twitter_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.DataFrameReader
import java.time.LocalDateTime

object Question4 {
  def run(spark: SparkSession) {
    import spark.implicits._

    val tweetDF = spark.read.json("tweetstream.tmp")
    tweetDF.createOrReplaceTempView("tweets")

    val userDF = spark.read.json("users.tmp")
    userDF.createOrReplaceTempView("users")

    val joinedDF = spark.sql("SELECT * FROM tweets INNER JOIN users ON users.id = tweets.data.author_id")
    joinedDF.createOrReplaceTempView("users_tweets")

    // separate data into positive or negative temp views 
    val positives = spark.sql("SELECT * FROM users_tweets WHERE matching_rules[0].tag LIKE 'positive%'")
    val negatives = spark.sql("SELECT * FROM users_tweets WHERE matching_rules[0].tag LIKE 'negative%'")
    positives.createOrReplaceTempView("pos_table")
    negatives.createOrReplaceTempView("neg_table")

    // designate older subscriptions to be created before 2019-01-01
    val oldPos = spark.sql("SELECT * FROM pos_table WHERE created_at < '2019-01-01'")
    val oldNeg = spark.sql("SELECT * FROM neg_table WHERE created_at < '2019-01-01'")
    val newPos = spark.sql("SELECT * FROM pos_table WHERE created_at > '2019-01-01'")
    val newNeg = spark.sql("SELECT * FROM neg_table WHERE created_at > '2019-01-01'")
    oldPos.createOrReplaceTempView("old_pos")
    oldNeg.createOrReplaceTempView("old_neg")
    newPos.createOrReplaceTempView("new_pos")
    newNeg.createOrReplaceTempView("new_neg")

    // total positive and negative tweets per old and non-new users
    val totalOldPos = spark.sql("SELECT count(data.id) FROM old_pos")
    val totalOldNeg = spark.sql("SELECT count(data.id) FROM old_neg")
    val totalNewPos = spark.sql("SELECT count(data.id) FROM new_pos")
    val totalNewNeg = spark.sql("SELECT count(data.id) FROM new_neg")

    val resultOldPos = totalOldPos.as[Long].collect()
    val resultOldNeg = totalOldNeg.as[Long].collect()
    val resultNewPos = totalNewPos.as[Long].collect()
    val resultNewNeg = totalNewNeg.as[Long].collect()

    println("Question 4")
    println(" ")
    println("Percentage of positive and negative tweets for older users")
    val resultsTableOlderUsers = Seq(
      Result(" Positive tweets older users ", (resultOldPos(0).toFloat / (resultOldPos(0) + resultOldNeg(0))) * 100),
      Result(" Negative tweets older users ", (resultOldNeg(0).toFloat / (resultOldPos(0) + resultOldNeg(0))) * 100),
      ).toDS()
    resultsTableOlderUsers.show()
    
    println("Percentage of positive and negative tweets for newer users")
    val resultsTableNewerUser = Seq(
      Result(" Positive tweets newer users ", (resultNewPos(0).toFloat / (resultNewPos(0) + resultNewNeg(0))) * 100),
      Result(" Negative tweets from newer users ", (resultNewNeg(0).toFloat / (resultNewPos(0) + resultNewNeg(0))) * 100)
      ).toDS()
    resultsTableNewerUser.show()

    // hopefully this is scoped only to this object, otherwise may shut down session for main
    spark.stop()
  }

  case class Result(results_description: String, percentage: Float)
}