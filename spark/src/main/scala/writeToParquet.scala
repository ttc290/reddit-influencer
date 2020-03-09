// spark-shell --jars ~/postgresql-42.2.9.jar
// spark-submit --class writeToParquet --master spark://ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com:7077 /home/ubuntu/Insight/target/scala-2.11/etl_2.11-1.0.jar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object writeToParquet {
	def main(args: Array[String]): Unit = {
		// create Spark session
		val spark = SparkSession.builder.appName("writeToParquet").getOrCreate()
		
		import spark.implicits._

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

		// write to Parquet
		sampleDF.write.mode("append").parquet("s3a://reddit-tc-parquet/reddit.parquet")

		// stop spark applicaiton
		spark.stop()
	}
}

