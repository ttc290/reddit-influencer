import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.util.Properties

object etl {
	def main(args: Array[String]): Unit = {
		// create Spark session
		val spark = SparkSession.builder.appName("etl").getOrCreate()
		
		import spark.implicits._

		// load raw data
		val sampleDF = spark.read.json("s3a://reddit-tc")

		// select only relevant columns
		val subColDF = sampleDF.select("author", "body", "created_utc", "distinguished", "score", "subreddit")

		// create a brand and product dataframe for testing
		//val brand = Seq("Apple").toDF("brand")
		//val product = Seq("iPhone").toDF("product")
		//val productDF = brand.crossJoin(product)
		//val productDF = Seq(("Apple", "iPhone")).toDF("brand", "product")

		// convert epoch time to EDT time zone
		val convertedTimeDF = subColDF.withColumn("created_time", from_utc_timestamp(from_unixtime($"created_utc"), "EDT"))

		// add year and month columns
		val addYearDF = convertedTimeDF.withColumn("year", year($"created_time"))
		val addMonthDF = addYearDF.withColumn("month", month($"created_time"))

		// remove empty comments
		val nonEmptyCommentDF = addMonthDF.filter(length($"body") > 0)

		// remove deleted accounts
		val existingAcctDF = nonEmptyCommentDF.filter($"author" =!= "[deleted]")

		// only search for comments in relevant subreddits
		val subredditDF = existingAcctDF.filter($"subreddit" isin ("gadgets", "technology", "apple", "iphone"))

		// only keep comments containing the keywords: Apple and iPhone
		val cleanDF = subredditDF.filter($"body".contains("Apple") && $"body".contains("iPhone"))

		// aggregate user's net-score (upvotes - downvotes)
		val aggScoreDF = cleanDF.select($"year", $"month", $"author", $"score").groupBy("year", "month", "author").sum("score")

		// add brand and product columns
		val addBrandDF = aggScoreDF.withColumn("brand", lit("Apple"))
		val output = addBrandDF.withColumn("product", lit("iPhone"))

		// convert output to Dataframe
		val outputPostgres = output.toDF()

		// JDBC configuration
		val prop = new java.util.Properties
		prop.setProperty("driver", "org.postgresql.Driver")
		prop.setProperty("user", "ubuntu")		

		// reddit is the database name
		val url = "jdbc:postgresql://<Postgres-server-ip>:5432/reddit"

		// table name
		val table = "reddit_analytics"

		// write result to Postgres
		outputPostgres.write.mode("append").jdbc(url, table, prop)

		// stop spark applicaiton
		spark.stop()
	}
}

