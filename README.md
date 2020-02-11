# Reddit Influencer Analytics

## Table of Contents
1. [Problem](README.md#problem)
1. [The approach](README.md#the-approach)
1. [Expected output](README.md#expected-output)
1. [Repo directory structure](README.md#repo-directory-structure)

## Problem

Reddit is one of the biggest social media platforms with a wide range of topics such as technology, fashion, politics. With 330M monthly active users, Reddit has the potential to influence the image of a company in the eyes of the public. However, actively promoting for a brand or product on the platform is generally not received well within the community. I would like to identify the core members within each community and provide a way for company to grow its influence from the inside.

## The approach

For each brand and product, I want to identify the 10 most influential Reddit users based on the score of comments mentioning the brand and product. I believe the result would be useful for marketers (especially in small companies) as they can try to include these users into their marketing campaign. MVP is the list of users for each product with the highest score and the score graph of all users associated with the product during the chosen month.

## Expected output

Output is the table of top 10 Reddit users ranked by their aggregated total scores for comments associated with the company's brand and product during the month. There is also a score graph of all users during the same month.

## Repo directory structure

The directory structure for the repo should look like this:

    ├── README.md
    ├── frontend
    │   └── frontend.py
    ├── ingestion
    │   └── ingest.sh
    ├── postgres
    │   └── brand_product.csv
    └── spark
        ├── built.sbt
        ├── src
        │   └── main
        │       └── scala
        │           └── etl.scala
        └── target
            └── scala-2.11
                ├── etl_2.11-1.0.jar
                └── postgresql-42.2.9.jar
        