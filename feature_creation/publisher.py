# Databricks notebook source
# MAGIC %md
# MAGIC ## `Publisher` Data Dictionary (all features created by inner joining parts of `checkout_ph` and `collection` on `Publisher`)
# MAGIC * **publisher_key**: Publisher name, the table's primary key. 
# MAGIC * **pub_tot_checkouts**: The total amount of checkouts per publisher. Created by grouping by publisher and doing a `count()`.
# MAGIC * **pub_tot_books**: The total about of books per publisher. The total amount of checkouts per publisher. Created by grouping by publisher and doing a `CountDistinct()` on BibNum.
# MAGIC * **pub_checkout_percentile**: The percentile of a publisher in regards to their total checkouts. Created by using the `percent_rank()` on pub_tot_checkouts.
# MAGIC * **pub_books_percentile**: The percentile of a publisher in regards to their total books. Created by using the `percent_rank()` on pub_tot_books.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating features by Publisher

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

# creating datatables from the database tables
collection = spark.table("library.collection")

checkout = spark.sql('''
SELECT *, MONTH(CheckoutDateTime) Month, YEAR(CheckoutDateTime) Year
FROM library.checkout_ph
'''
)

dictionary = spark.sql("SELECT * FROM library.data_dictionary").toPandas()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## filtering checkout data to sept 2017 to sept 2019

# COMMAND ----------

checkout.filter("((Year = 2017 and Month >= 9) or (Year >= 2018)) and ((Year = 2019 and Month <= 9) or (year < 2019))")

# COMMAND ----------

df_print_books = dictionary.query("code_type == 'ItemType' and format_subgroup == 'Book' and format_group == 'Print'")
book_codes = df_print_books.code.to_list()
print(book_codes)

# display(df_print_books)

# COMMAND ----------

books = collection \
  .withColumn('ReportDate', F.to_date(F.col("ReportDate"), "MM/dd/yyyy")) \
  .filter((F.col('ReportDate') >= F.lit("2017-09-01")) & (F.col('ReportDate') <= F.lit("2019-09-30")) & (F.col('ItemType').isin(book_codes)))
#display(books)

# COMMAND ----------

display(checkout)

# COMMAND ----------

display(books)

# COMMAND ----------

checkout_join = checkout.select(checkout.BibNumber)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## creating my dataset with publisher to create features

# COMMAND ----------

# creating my dataset with publisher to create features
pub = books.join(checkout_join, checkout_join.BibNumber ==  books.BibNum, "inner") 

pub = pub.select(["BibNum", "Publisher", "PublicationYear", "Title"])

# COMMAND ----------

display(pub)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## creating a total checkouts by publisher in the SPL system column

# COMMAND ----------

# creating a total checkouts by publisher feature
chkts_per_pub = pub.groupby("Publisher").count()

# COMMAND ----------

chkts_per_pub.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##creating a total unique bib numbers by publisher in the SPL system column

# COMMAND ----------

# creating total books by publisher
books_per_pub = books.groupBy("Publisher").agg(countDistinct(col("BibNum")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## joining the two columns

# COMMAND ----------

# joining features into main dataset

publisher = chkts_per_pub.join(books_per_pub, ["Publisher"], "outer")

# COMMAND ----------

publisher = publisher.withColumnRenamed("count", "total_checkouts")
publisher = publisher.withColumnRenamed("count(BibNum)", "total_books")

# COMMAND ----------

# replacing null's with 0's
publisher = publisher.na.fill(value=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## creating publisher rank in system in terms of checkout totals

# COMMAND ----------

windowSpec = Window \
.partitionBy() \
.orderBy(col("total_checkouts"))

publisher = publisher \
.withColumn("chkts_percent_rank", percent_rank().over(windowSpec))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## creating publisher rank in system in terms of bibnumber totals

# COMMAND ----------

windowSpec = Window \
.partitionBy() \
.orderBy(col("total_books"))



publisher = publisher \
.withColumn("books_percent_rank", percent_rank().over(windowSpec))

# COMMAND ----------

publisher = publisher.withColumnRenamed("total_checkouts", "pub_tot_checkouts")
publisher = publisher.withColumnRenamed("total_books", "pub_tot_books")
publisher = publisher.withColumnRenamed("chkts_percent_rank", "pub_checkout_percentile")
publisher = publisher.withColumnRenamed("books_percent_rank", "pub_books_percentile")

# COMMAND ----------

publisher = publisher.withColumnRenamed("Publisher", "publisher_key")

# COMMAND ----------

display(publisher)

# COMMAND ----------

# MAGIC %md
# MAGIC ## creating a `publisher` table in `library_features` and populating it with `publisher` data

# COMMAND ----------

publisher.write.saveAsTable("library_features.publisher")
