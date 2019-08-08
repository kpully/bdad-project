// imports

//load data from HDFS
val insta = sc.textFile("instagram_comments.csv").map(line=>line.split(",")).filter(line=>line.length==7)
val data = insta.map(row=>(row(0),row(1), row(2), row(3),row(4),row(5),row(6))).distinct

//view data
data.take(10).foreach(println) 
// get size of data
data.count
// convert to dataframe
data_df = data.toDF("brand","post_ts", "comment_ts", "comment_id", "username", "user_id", "comment")
// get schema
data_df.schema

//profile data
//get lengths of comments in characters
val comment_lengths = data.map(line=>line._7.length)
comment_lengths.countByValue()
// convert to dataframe
val comment_lengths_df = comment_lengths.toDF("comment_length")
// get maximum comment length
val max_comment_length = comment_lengths_df.sort($"comment_length".desc).first()
max_comment_length
// get minimum commewnt length
val min_comment_length = comment_lengths_df.sort($"comment_length".asc).first()
min_comment_length

//get distribution of timestamps
val comment_timestamps = data.map(line=>line.split(",")(3))

// see which brand get the most comments
val comments_by_brand = data.map(line=>(line._1,1)).reduceByKey((v1,v2)=>v1+v2)
// convert to dataframe
val comments_by_brand_df = comments_by_brand.toDF("brand", "comments")
// get brand with most comments
val max_brand = comments_by_brand_df.sort($"comments".desc).first()
max_brand
// get brand with least comments
val min_brand = comments_by_brand_df.sort($"comments".asc).first()
min_brand

// range of post dates
// see most recent post timestamp
val max_post_ts = data_df.sort("$post_ts".desc).first()
// see how far back posts go using post timestamp
val min_post_ts = data_df.sort("$post_ts".asc).first()

//see which users comment the most
val users = data.map(line=>line._5)
users.countByValue()

//output clean dataset
filtered.saveAsTextFile("instagram_data/csv/clean")