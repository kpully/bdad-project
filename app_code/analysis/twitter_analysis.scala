// to run:
// spark-shell
// :load script.scala

import org.apache.spark.{SparkContext, SparkConf};
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType};
import org.apache.spark.sql.Row;

def is_valid(brand: String) = {
  if (brand=="glossier" || brand=="honest" || brand=="fentybeauty" || brand=="sephora" 
    || brand=="benefitcosmetics" || brand=="maccosmetics" || brand=="bumbleandbumble"
    || brand=="kyliecosmetics" || brand=="chanel" || brand=="UrbanDecay" || brand=="BobbieBrown"
    || brand=="patmcgrath" || brand == "milkmakeup" || brand=="madisonreedllb" || brand =="SkinLaundry"
     ) {
    true;
  } else {
    false;
  }
}

def is_brand(brand: String) = {
  brand.replaceAll("#","").replaceAll("@","");
  if (brand=="glossier" || brand=="honest" || brand=="fentybeauty" || brand=="sephora" 
    || brand=="benefitcosmetics" || brand=="maccosmetics" || brand=="bumbleandbumble"
    || brand=="kyliecosmetics" || brand=="chanel" || brand=="UrbanDecay" || brand=="BobbieBrown"
    || brand=="patmcgrath" || brand == "milkmakeup" || brand=="madisonreedllb" || brand =="SkinLaundry"
     ) {
    true;
  } else {
    false;
  }
}

val swfile = "stopwords.csv"
val sw = sc.textFile(swfile)
val stopWords = sw.flatMap(x => x.split(",")).map(_.trim.replace("\"", ""))

def not_stopWord(word:String) = {
  if (stopWords.collect contains word) false
  else true
}
val address = "tw_data/*"
val csv = sc.textFile(address)
csv.persist

val rows = csv.map(line => line.split(",").map(_.trim.replace("\"", ""))).filter(line=>line.length>6)
val header = rows.first
val alldata = rows.filter(row=>is_valid(row(0))==true && row(0) != header(0))


val rel_data = alldata.map(values=>
        (values(0),                                     //brand
        values(1),                                      //query
        values(2),                                      //user_id
        values(3),                                      //status_id (tweet)
        values(4).split('-')(0),                        //created_at (tweet)
        values(5),                                      //screen_name (user)
        values(6)))                                     //tweet text 

val data = rel_data.distinct

// functions
def is_query(brand: String, text: String) = { text contains ("@"+brand) }

def has_emoji(text: String) = { text contains ("❤️") }
def has_smiley(text: String) = { text contains ("😍") }

def is_hashtag(brand: String, comment: String) = { comment contains ("#"+brand) }

// distribution of tweets by brand
//data.map(_(0)).countByValue()
val brand_entries = data.map(line=>(line._1,1)).reduceByKey((v1,v2)=>v1+v2)
brand_entries.collect.foreach(println)

val brand_query_count = data.filter(row=>is_query(row._1,row._2)).map(line=>(line._1,1)).reduceByKey((v1,v2)=>v1+v2)
brand_query_count.collect.foreach(println)

val brand_hashtag_count = data.filter(row=>is_hashtag(row._1,row._2)).map(line=>(line._1,1)).reduceByKey((v1,v2)=>v1+v2)
brand_hashtag_count.collect.foreach(println)

// emojis do not render, and cannot be matched even under the hood :(
val brand_emoji_count = data.filter(row=>has_emoji(row._7)).map(line=>(line._1,1)).reduceByKey((v1,v2)=>v1+v2)
brand_emoji_count.collect.foreach(println)

val smile = data.filter(row=>has_smiley(row._7)).map(line=>(line._1,1)).reduceByKey((v1,v2)=>v1+v2)
smile.collect.foreach(println)

//overall word (entire corpus)
val words = data.flatMap(line=>(line._7.split(" ")).filter(_.length>2).map(word=>(word.toLowerCase)))
words.take(10).foreach(println)
//val wo = words.filter(x => not_stopWord())

//overall corpus
val word_count = words.map(word=>(word, 1))
val word_freq = word_count.reduceByKey(_ + _).sortBy(_._2,false)
word_freq.take(10).foreach(println)

val words = data.flatMap(line=>(line._7.split(" ")).filter(_.length>2).map(word=>(word.toLowerCase.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", ""),line._1)))

//tfidf
// term frequency - words associated with brand
val tweetFreq = words.map(line => (line,1))
val tf = tweetFreq.reduceByKey(_ + _).sortBy(_._2,false)
tf.take(20).foreach(println)
val tweetFreq = words.map(line => (line,1))
tweetFreq.take(20).foreach(println)
// term frequency by brand
val tf = tweetFreq.reduceByKey(_ + _).sortBy(_._2,false)
tf.take(20).foreach(println)
// group by brand
val freqwords_by_brand = tf.map(line=>(line._1._2,line._1._2,line._2))
// brand frequency by term 

val df = words.map(_._1).countByValue().sortBy(_._2,false)
df.take(20).foreach(println)

//cross brand analysis

// cross brand POSTS
// (word,brand,post)
val posts = data.flatMap(line=>(line._7.split(" ")).filter(_.length>2).map(word=>(word.toLowerCase.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", ""),line._1,line._4)))
val brand_directed_posts = posts.filter(word=>is_brand(word._1) && word._1.replaceAll("#","").replaceAll("@","") != word._2).distinct.map(line=>(line,1))
val brand_directed_posts_count = brand_directed_posts.map(row=>((row._1._1,row._1._2),row._2)).reduceByKey(_ + _).sortBy(_._2,false).map(row=>(row._1._2,(row._1._1,row._2)))

val cross_as_percent_of_posts = brand_directed_posts_count.join(brand_entries)
val cross_percent_posts = cross_as_percent_of_posts.map(line=>(line._1,line._2._1._1,line._2._1._2.toDouble, line._2._2.toDouble, line._2._1._2.toDouble/line._2._2.toDouble))
// DISPLAY for Tableau
val crossBrand = cross_percent_posts.map(line=>(line._1,line._2,line._5))
crossBrand.saveAsTextFile("crossBrand.csv")

// cross brand WORDS exploration
val crossFreq = brand_directed.map(line => (line,1))
val crossFreq_Count = crossFreq.reduceByKey(_ + _).sortBy(_._2,false)
crossFreq_Count.collect.foreach(println)
val num_words_by_brand = data.flatMap(line=>(line._7.split(" ")).filter(_.length>2).map(word=>(line._1,1)))
// as percent of words
val crossFreq_as_percent = crossFreq_Count.map(line=>line._1._1).reduceByKey(_ + _).sortBy(_._2,false)
// # Brand post / total posts (by brand i.e.  = glossier posts with other brand mentions / glossier posts)
val CF = crossFreq_Count.map(line=>(line._1._1,(line._1._2,line._2)))
val crossFreq_as_percent_of_tweets = CF.join(brand_entries).map(line=>(line._1,line._2._1._1,line._2._1._2.toDouble / line._2._2.toDouble))


// most popular users (repeat posters --> greater than 1 post)
val brand_users = data.map(line=>((line._1,line._6),1)).reduceByKey((v1,v2)=>v1+v2).map(line=>(line._1._1,(line._1._2,line._2))).filter(line=>line._2._2>1).groupByKey
val brand_topuser = brand_users.map(row=>(row._1,row._2.reduce((acc,value) => { 
  if(acc._2 < value._2) value else acc})))
brand_users.take(10).foreach(println)
brand_topuser.collect.foreach(println)
// DISPLAY for Tableau
brand_topuser.saveAsTextFile("top_users.csv")

// comments of top users
val comKeys = brand_topuser.map(row=>(row._1,row._2._1)).collect.toSet
val comments = data.map(row=>((row._1,row._6),row._7)).filter{case (key, d) => comKeys.contains(key)}
comments.saveAsTextFile("top_users_comments.csv")
