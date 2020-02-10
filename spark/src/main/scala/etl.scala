// spark-shell --jars /home/ubuntu/reddit-influencer/spark/target/scala-2.11/postgresql-42.2.9.jar
// spark-submit --class etl --num-executors 3 --executor-cores 6 --executor-memory 6G --master spark://<master-node-ip>:7077 --jars /home/ubuntu/reddit-influencer/spark/target/scala-2.11/postgresql-42.2.9.jar /home/ubuntu/reddit-influencer/spark/target/scala-2.11/etl_2.11-1.0.jar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
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

	def analyze (df: DataFrame): DataFrame = {
		// only search for comments in relevant subreddits
		val subredditDF = df.filter(col("subreddit") isin ("gadgets", "technology", lower(regexp_replace(col("brand"), " ", "")), lower(regexp_replace(col("product"), " ", ""))))

		// keep relevant columns for final result
		val finalDF = subredditDF.select(col("year"), col("month"), col("author"), col("score"), col("brand"), col("product"))

		// aggregate user's net-score (upvotes - downvotes)
		val aggScoreDF = finalDF.groupBy("year", "month", "author", "brand", "product").sum("score").withColumnRenamed("sum(score)", "score")
		
		// convert output to Dataframe
		val output = aggScoreDF.toDF()

		output
	}

	def writeToDB (df: DataFrame, tableName: String) = {
		// JDBC prop
		val prop = new java.util.Properties
		prop.setProperty("driver", "org.postgresql.Driver")
		prop.setProperty("user", "ubuntu")		

		// ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com is the PostgreSQL server
		// reddit is the database name
		val destination = "ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com"
		val dbName = "reddit"
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
		prop.setProperty("user", "ubuntu")		

		// ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com is the PostgreSQL server
		// reddit is the database name
		val destination = "ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com"
		val dbName = "reddit"
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
		val sampleDF = spark.read.schema(custom_schema).json("s3a://reddit-tc")
		
		// clean raw data
		val cleanDF = preprocess(sampleDF)

		// read brand and product from database
		val brandProductDF = readFromDB("brand_product")

		// scan 'body' column in cleanDF for brand and product in each row of brandProductDF and join the 2 tables
		val workingDF = cleanDF.join(brandProductDF, col("body").contains(col("brand")) && col("body").contains(col("product")))

		// analyze the data
		val output = analyze(workingDF).persist()
		
		// save result to Postgres
		writeToDB(output, "reddit_users_tuning")

		// stop spark applicaiton
		spark.stop()
	}
}

