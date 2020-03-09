## PostgreSQL database

1. Populate the `brand_product` table in PostgreSQL by importing the `brand_product.csv` file

2. The resulting tables after ETL process from Spark are: `reddit_users_score` (storing user's aggregated score) and `reddit_users_comment` (storing user's top comment and sentiment analysis)

3. Run `master_table.sql` to create `master_table` by joining the 2 tables.

## Table schema

1. `brand_product`

```
 |-- brand: text (nullable = true)
 |-- product: text (nullable = true)
```

2. `reddit_users_score`

```
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- author: text (nullable = true)
 |-- brand: text (nullable = true)
 |-- product: text (nullable = true)
 |-- score: bigint (nullable = true)
```

3. `reddit_users_comment`

```
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- author: text (nullable = true)
 |-- brand: text (nullable = true)
 |-- product: text (nullable = true)
 |-- score: bigint (nullable = true)
 |-- text: text (nullable = true)
 |-- sentiment_result: text (nullable = true)
```

4. `master_table`

```
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- author: text (nullable = true)
 |-- maxScore: bigint (nullable = true)
 |-- score: bigint (nullable = true)
 |-- body: text (nullable = true)
 |-- sentiment: text (nullable = true)
 |-- brand: text (nullable = true)
 |-- product: text (nullable = true)
```