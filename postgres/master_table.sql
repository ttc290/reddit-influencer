create table master_table as 
(select t1.year, t1.month, t1.author, t1.score maxScore, t2.score, t1.text body, t1.sentiment_result sentiment, t1.brand, t1.product 
from reddit_users_comment t1, reddit_users_score t2 
where t1.author = t2.author and t1.year = t2.year and t1.month = t2.month and t1.brand = t2.brand and t1.product = t2.product);