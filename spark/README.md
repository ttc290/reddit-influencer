## How to run ETL job

1. Download JDBC Driver:

`wget https://jdbc.postgresql.org/download/postgresql-42.2.9.jar`

2. Build ETL application JAR using SBT:

`sbt package`

3. Submit ETL job to Spark cluster:

`spark-submit --class etl --master spark://<master-node-ip>:7077 --jars spark/target/scala-2.11/postgresql-42.2.9.jar spark/target/scala-2.11/etl_2.11-1.0.jar`