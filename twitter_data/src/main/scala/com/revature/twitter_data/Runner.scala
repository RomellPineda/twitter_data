package twitter_data

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.DataFrameReader
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

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("twitter_data")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    helloTweetStream(spark)
  }

  def helloTweetStream(spark: SparkSession): Unit = {
    import spark.implicits._
    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))
    val tweetstreamURI = "https://api.twitter.com/2/tweets/sample/stream"
    val userURI = "https://api.twitter.com/2/users"
    val testUserIdString = "?ids=830480558597763075,150942805,1479694148,1284732561927929856,880545236732313600"

    // tweetStreamToDir(bearerToken, uriString = tweetstreamURI + "?expansions=author_id")
    tweetStreamToDir(bearerToken, dirname = "users", uriString = userURI + testUserIdString + "&user.fields=created_at,verified")
  }

  def tweetStreamToDir(
      bearerToken: String,
      dirname: String = "twitterstream",
      linesPerFile: Int = 1000,
      uriString: String = ""
  ) = {

    // AWS configs
    val bucketName = "usf-210104-big-data"
    val key = System.getenv(("DAS_KEY_ID"))
    val secret = System.getenv(("DAS_SEC"))

    // AWS client set up
    val awsCredentials = new BasicAWSCredentials(key, secret)
    val amazonS3Client = new AmazonS3Client(awsCredentials)

    val httpClient = HttpClients.custom
      .setDefaultRequestConfig(
        RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
      )
      .build()
    val uriBuilder: URIBuilder = new URIBuilder(
      uriString
    )
    val httpGet = new HttpGet(uriBuilder.build())
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()

    if (dirname == "users")

    if (null != entity) {
      val userData = EntityUtils.toString(entity, "UTF-8")
      var fileWriter = new PrintWriter(Paths.get("users.tmp").toFile)
      fileWriter.write(userData)
      fileWriter.flush()
      fileWriter.close()

      val millis = System.currentTimeMillis()
      val folderName = s"$dirname/data-$millis"
      var fileToUpload = new File("users.tmp")
      amazonS3Client.putObject(bucketName, folderName, fileToUpload)
    }

    else

    if (null != entity) {
      val reader = new BufferedReader(
        new InputStreamReader(entity.getContent())
      )
      var line = reader.readLine()
      var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
      var lineNumber = 1
      val millis = System.currentTimeMillis()
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          val fileName = s"$dirname/tweetstream-$millis-${lineNumber/linesPerFile}"
          Files.move(
            Paths.get("tweetstream.tmp"),
            Paths.get(fileName))
          fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
          var fileToUpload = new File(fileName)
          amazonS3Client.putObject(bucketName, fileName, fileToUpload)
        }

        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }
    }
  }
}