package twitter_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.DataFrameReader

object Question3 {
  /*
  Answers question three by observing pos/neg distribution of tweets from both
  active users
  &
  inactive users
  */
  def run(spark: SparkSession) {
    import spark.implicits._
    val data = spark.read.json("filteredDataSample")

    data.createOrReplaceTempView("tweetz")

    // calculate average tweets per user within filteredDataSample timeframe
    // nested into user tweet count to differentiate between active and less active users
    // val averageTweetsPerUser = spark.sql("SELECT avg(tweet_totals) FROM (SELECT count(data.author_id) AS tweet_totals FROM tweetz GROUP BY data.author_id)")

    // separate data into positive or negative temp views 
    val positives = spark.sql("SELECT * FROM tweetz WHERE matching_rules[0].tag LIKE 'positive%'")
    val negatives = spark.sql("SELECT * FROM tweetz WHERE matching_rules[0].tag LIKE 'negative%'")
    positives.createOrReplaceTempView("pos_table")
    negatives.createOrReplaceTempView("neg_table")

    // separate positive and negative temp views by active and less active users
    val activePos = spark.sql("SELECT data.author_id, count(data.author_id) AS active_pos_count FROM pos_table GROUP BY data.author_id HAVING count(data.author_id) > (SELECT avg(tweet_totals) FROM (SELECT count(data.author_id) AS tweet_totals FROM tweetz GROUP BY data.author_id))")
    val activeNeg = spark.sql("SELECT data.author_id, count(data.author_id) AS active_neg_count FROM neg_table GROUP BY data.author_id HAVING count(data.author_id) > (SELECT avg(tweet_totals) FROM (SELECT count(data.author_id) AS tweet_totals FROM tweetz GROUP BY data.author_id))")
    val nonactivePos = spark.sql("SELECT data.author_id, count(data.author_id) AS nonactive_pos_count FROM pos_table GROUP BY data.author_id HAVING count(data.author_id) < (SELECT avg(tweet_totals) FROM (SELECT count(data.author_id) AS tweet_totals FROM tweetz GROUP BY data.author_id))")
    val nonactiveNeg = spark.sql("SELECT data.author_id, count(data.author_id) AS nonactive_neg_count FROM neg_table GROUP BY data.author_id HAVING count(data.author_id) < (SELECT avg(tweet_totals) FROM (SELECT count(data.author_id) AS tweet_totals FROM tweetz GROUP BY data.author_id))")
    activePos.createOrReplaceTempView("act_pos")
    activeNeg.createOrReplaceTempView("act_neg")
    nonactivePos.createOrReplaceTempView("nonactive_pos")
    nonactiveNeg.createOrReplaceTempView("nonactive_neg")

    // total positive and negative tweets per active and non-active users
    val totalActivePos = spark.sql("SELECT sum(active_pos_count) FROM act_pos")
    val totalActiveNeg = spark.sql("SELECT sum(active_neg_count) FROM act_neg")
    val totalNonActivePos = spark.sql("SELECT sum(nonactive_pos_count) FROM nonactive_pos")
    val totalNonActiveNeg = spark.sql("SELECT sum(nonactive_neg_count) FROM nonactive_neg")

    val resultActivePos = totalActivePos.as[Long].collect()
    val resultActiveNeg = totalActiveNeg.as[Long].collect()
    val resultNonActivePos = totalNonActivePos.as[Long].collect()
    val resultNonActiveNeg = totalNonActiveNeg.as[Long].collect()

    println(" ")
    println("Results for users with above average tweet activity---")
    val resultsTableActiveUsers = Seq(Result(" Positive tweets active users ", (resultActivePos(0).toFloat / (resultActivePos(0) + resultActiveNeg(0))) * 100),Result(" Negative tweets active users ", (resultActiveNeg(0).toFloat / (resultActivePos(0) + resultActiveNeg(0))) * 100)).toDS()
    resultsTableActiveUsers.show()
      
      
    println("Results for users with below average tweet activity---")
    val resultsTableNonActiveUser = Seq(
      Result(" Positive tweets non-active users ", (resultNonActivePos(0).toFloat / (resultNonActivePos(0) + resultNonActiveNeg(0))) * 100),Result(" Negative tweets from non-active users ", (resultNonActiveNeg(0).toFloat / (resultNonActivePos(0) + resultNonActiveNeg(0))) * 100)).toDS()
    resultsTableNonActiveUser.show()

    // hopefully this is scoped only to this object, otherwise may shut down session for main
    //spark.stop()
  }

  case class Result(results_description: String, percentage: Float)
}