// attempts to create using dataframe schema

// to run:
// spark-shell
// :load script.scala

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.SparkContext, SparkConf
import org.apache.spark.SQLContext,Row

conf = SparkConf().setAppName("finalProjectApp").setMaster("local")

sc = SparkContext(conf=conf) 

sqlContext = sql.SQLContext(sc) 

val address = "tw_data/*"
val data = sc.textFile(address)
data.persist

val filled = data.filter(line => line.size > 0)
val csv = filled.map(line => line.split(","))
val header = csv.first
val headerColumns = data.first().split(",").to[List]

val dfSchema = new StructType()
dfSchema.add("brand",StringType)
dfSchema.add("query",StringType)
dfSchema.add("user_id",StringType)
dfSchema.add("status_id",StringType)
dfSchema.add("created_at",StringType)
dfSchema.add("screen_name",StringType)
dfSchema.add("tweet_text",StringType)
dfSchema.add("favorite_count",StringType)
dfSchema.add("retweet_count",StringType)
dfSchema.add("name",StringType)
dfSchema.add("description",StringType)
dfSchema.add("followers_count",StringType)

// def dfSchema(columnNames: List[String]): StructType = {
//   StructType(
//     Seq(
//       StructField(name = "brand", dataType = StringType, nullable = false),
//       StructField(name = "query", dataType = StringType, nullable = false),
//       StructField(name = "user_id", dataType = StringType, nullable = false),
//       StructField(name = "status_id", dataType = StringType, nullable = false),
//       StructField(name = "created_at", dataType = StringType, nullable = false),
//       StructField(name = "screen_name", dataType = StringType, nullable = false),
//       StructField(name = "tweet_text", dataType = StringType, nullable = false),
//       StructField(name = "favorite_count", dataType = StringType, nullable = false),
//       StructField(name = "retweet_count", dataType = StringType, nullable = false),
//       StructField(name = "name", dataType = StringType, nullable = false)
//       StructField(name = "description", dataType = StringType, nullable = false)
//       StructField(name = "followers_count", dataType = StringType, nullable = false)
//     )
//   )
// }

def row(values: List[String]): Row = {
  Row(
  		values(0),                                     //brand
        values(1),                                      //query
        values(2),                                      //user_id
        values(3),                                      //status_id (tweet)
        values(4).split('-')(0),                        //created_at (tweet)
        values(5),                                      //screen_name (user)
        values(6),                                      //tweet text
        values(14),                                     //favorite_count (tweet)
        values(15),                                     //retweet_count (tweet)
        values(57),                                     //name (user)
        values(59),                                     //description (user)
        values(62))
}

// // define a schema for the file

val schema = dfSchema(headerColumns)

val data2 =
  rdd
    .mapPartitionsWithIndex((index, element) => if (index == 0) it.drop(1) else it) // skip header
    .map(_.split(",").to[List])
    .map(row)

val dataFrame = sqlContext.createDataFrame(data2, schema)

val content = csv.filter(line => line != header)
content.take(10).foreach(println)

//val dataFrame = sqlContext.createDataFrame(data, dfSchema)

val rel_cols = content.map(values=>
        (values(0),                                     //brand
        values(1),                                      //query
        values(2),                                      //user_id
        values(3),                                      //status_id (tweet)
        values(4).split('-')(0),                        //created_at (tweet)
        values(5),                                      //screen_name (user)
        values(6),                                      //tweet text
        values(14),                                     //favorite_count (tweet)
        values(15),                                     //retweet_count (tweet)
        values(57),                                     //name (user)
        values(59),                                     //description (user)
        values(62)))                                    //followers_count (user)

//val reduced_data = rel_cols.distinct

def is_query(brand: String, text: String) = { text contains ("@"+brand) }
 
def is_hashtag(brand: String, comment: String) = { comment contains ("#"+brand) }

def comment_count_by_brand = reduced_data.map(line=>(line._1,1)).reduceByKey((v1,v2)=>v1+v2)

// sentiment analysis
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
import spark.implicits._

val df = sqlContext.createDataFrame(rel_cols)
//manually label data

// val regexTokenizer = new RegexTokenizer().setInputCol("_7").setOutputCol("words").setPattern("\\W")
// val countTokens = udf { (words: Seq[String]) => words.length }
// val regexTokenized = regexTokenizer.transform(df)
// regexTokenized.select("_7", "words").withColumn("tokens", countTokens(col("words"))).show(false)
// val result = regexTokenized.select("label","words")