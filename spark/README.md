## How to use Spark-NLP's pre-trained Sentiment Analysis pipeline offline

1. Download pre-trained Sentiment Analysis pipeline and save in `spark\target\scala-2.11\spark_nlp\sentiment_analysis`:

`wget https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/models/analyze_sentiment_en_2.4.0_2.4_1580483464667.zip`

2. Unzip the file to create `metadata` and `stages` folders.

## How to run ETL job

1. Download JDBC Driver JAR and put it in `spark\target\scala-2.11`:

`wget https://jdbc.postgresql.org/download/postgresql-42.2.9.jar`

2. Build ETL application JAR using SBT (`etl_2.11-1.0.jar` will be created in `spark\target\scala-2.11`):

`sbt package`

3. Run writeToParquet job to convert JSON files to Parquet format:

`spark-submit --class writeToParquet --master spark://<master-node-ip>:7077 spark/target/scala-2.11/etl_2.11-1.0.jar`

4. Run ETL job on Spark cluster and save results in PostgreSQL:

`spark-submit --class etl --master spark://<master-node-ip>:7077 --packages com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.2 --jars spark/target/scala-2.11/postgresql-42.2.9.jar spark/target/scala-2.11/etl_2.11-1.0.jar`