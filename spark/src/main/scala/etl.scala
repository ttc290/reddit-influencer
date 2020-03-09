// spark-shell --jars /home/ubuntu/reddit-influencer/spark/target/scala-2.11/postgresql-42.2.9.jar
// spark-submit --class etl --num-executors 3 --executor-cores 6 --executor-memory 6G --master spark://<master-node-ip>:7077 --packages com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.2 --jars /home/ubuntu/reddit-influencer/spark/target/scala-2.11/postgresql-42.2.9.jar /home/ubuntu/reddit-influencer/spark/target/scala-2.11/etl_2.11-1.0.jar

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.util.Properties

object etl {
	def preprocess (df: DataFrame): DataFrame = {
		// select only relevant columns
		val subColDF = df.select("author", "body", "created_utc", "distinguished", "score", "subreddit")

		// convert epoch time to EDT
		val convertedTimeDF = subColDF.withColumn("created_time", from_utc_timestamp(from_unixtime(col("created_utc")), "EDT"))

		// add year and month columns
		val addYearMonthDF = convertedTimeDF.withColumn("year", year(col("created_time"))).withColumn("month", month(col("created_time")))

		// remove empty comments
		val nonEmptyCommentDF = addYearMonthDF.filter(length(col("body")) > 0)

		// remove deleted accounts
		val existingAcctDF = nonEmptyCommentDF.filter(col("author") =!= "[deleted]")

		// only keep comments with score > 1
		val positiveCommentDF = existingAcctDF.filter(col("score") > 1)

		positiveCommentDF
	}

	def filterByBrandProduct (df: DataFrame): DataFrame = {
		// only search for comments in relevant subreddits
		val subredditDF = df.filter(col("subreddit") isin ("gadgets", "technology", lower(regexp_replace(col("brand"), " ", "")), lower(regexp_replace(col("product"), " ", ""))))

		// keep relevant columns for final result
		val finalDF = subredditDF.select(col("year"), col("month"), col("author"), col("body").alias("text"), col("score"), col("brand"), col("product"))
		
		finalDF
	}

	def aggScore (df: DataFrame): DataFrame = {
		// aggregate user's scores
		val aggScoreDF = df.groupBy("year", "month", "author", "brand", "product").sum("score").withColumnRenamed("sum(score)", "score")
		
		aggScoreDF
	}

	def highestScore (df: DataFrame): DataFrame = {
		// define partition columns
		val maxScoreWindow = Window.partitionBy("year", "month", "author", "brand", "product")

		// get user's comment with highest score
		val highScoreCommentDF = df.withColumn("maxScore", max("score").over(maxScoreWindow)).filter(col("maxScore") === col("score")).drop("score").withColumnRenamed("maxScore", "score")

		// load pre-trained Sentiment Analysis pipeline
		val sentiment_analysis = PretrainedPipeline.fromDisk("reddit-influencer/spark/target/scala-2.11/spark_nlp/sentiment_analysis")

		// analyze sentiment of each word in the 'text' column
		val sentiment = sentiment_analysis.transform(highScoreCommentDF)

		// counts of sentiment (positive, negative)
		val sentiment_expode = sentiment.select(col("year"), col("month"), col("author"), col("brand"), col("product"), col("score"), col("text"), explode(col("sentiment.result")).as("sentiment_result")).groupBy("year", "month", "author", "brand", "product", "score", "text", "sentiment_result").count
		
		// define partition columns
		val aggSentimentWindow = Window.partitionBy("year", "month", "author", "brand", "product", "score", "text")
		
		// define overall sentiment from sentiment's count (max)
		val commentSentimentDF = sentiment_expode.withColumn("maxCount", max("count").over(aggSentimentWindow)).filter(col("maxCount") === col("count")).drop("maxCount", "count")

		commentSentimentDF
	}

	def writeToDB (df: DataFrame, tableName: String) = {
		// JDBC prop
		val prop = new java.util.Properties
		prop.setProperty("driver", "org.postgresql.Driver")
		prop.setProperty("user", "xxxxxx")		

		// ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com is the PostgreSQL server
		// xxxxxx is the database name
		val destination = "ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com"
		val dbName = "xxxxxx"
		val url = s"jdbc:postgresql://$destination:5432/$dbName"

		// write result to PostgreSQL
		df.write.mode("append").jdbc(url, tableName, prop)
	}

	def readFromDB (tableName: String): DataFrame = {
		// create Spark session
		val spark = SparkSession.builder.appName("etl").getOrCreate()
		
		import spark.implicits._

		// JDBC prop
		val prop = new java.util.Properties
		prop.setProperty("driver", "org.postgresql.Driver")
		prop.setProperty("user", "xxxxxx")		

		// ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com is the PostgreSQL server
		// xxxxxx is the database name
		val destination = "ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com"
		val dbName = "xxxxxx"
		val url = s"jdbc:postgresql://$destination:5432/$dbName"

		// table name
		val query = s"(select * from $tableName) as test_query"

		// read from PostgreSQL
		val output = spark.read.jdbc(url, query, prop)
	
		output
	}

	def main(args: Array[String]): Unit = {
		// create Spark session
		val spark = SparkSession.builder.appName("etl").getOrCreate()
		
		import spark.implicits._

		// data schema of raw data
		val custom_schema = StructType(Seq(
			StructField("archived", BooleanType, true),
			StructField("author", StringType, true),
			StructField("author_flair_css_class", StringType, true),
			StructField("author_flair_text", StringType, true),
			StructField("body", StringType, true),
			StructField("controversiality", LongType, true),
			StructField("created_utc", StringType, true),
			StructField("distinguished", StringType, true),
			StructField("downs", LongType, true),
			StructField("edited", StringType, true),
			StructField("gilded", LongType, true),
			StructField("id", StringType, true),
			StructField("link_id", StringType, true),
			StructField("name", StringType, true),
			StructField("parent_id", StringType, true),
			StructField("permalink", StringType, true),
			StructField("retrieved_on", LongType, true),
			StructField("score", LongType, true),
			StructField("score_hidden", BooleanType, true),
			StructField("stickied", BooleanType, true),
			StructField("subreddit", StringType, true),
			StructField("subreddit_id", StringType, true),
			StructField("ups", LongType, true)))

		// load raw data
		val rawDF = spark.read.schema(custom_schema).parquet("s3a://reddit-tc-parquet/reddit.parquet/")
		
		// clean raw data
		val cleanDF = preprocess(rawDF)

		// read brand and product from database
		val brandProductDF = readFromDB("brand_product")

		// scan 'body' column in cleanDF for brand and product in each row of brandProductDF and join the 2 tables
		val filterJoinDF = cleanDF.join(brandProductDF, col("body").contains(col("brand")) && col("body").contains(col("product")))

		// filter comments containing brands and products
		val finalDF = filterByBrandProduct(filterJoinDF).persist()

		// table of user's aggregated score
		val scoreDF = aggScore(finalDF).persist()

		// table of user's comment with highest score
		val commentDF = highestScore(finalDF).persist()
		
		// save resulting tables to Postgres
		writeToDB(scoreDF, "reddit_users_score")
		writeToDB(commentDF, "reddit_users_comment")

		// stop spark applicaiton
		spark.stop()
	}
}