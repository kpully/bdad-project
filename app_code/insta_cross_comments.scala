//cross commenting module
//cross commenting in this case means instagram users commenting on multiple brands' posts

import org.apache.spark.rdd.RDD

// rdd of (username, (brands that user has commented on))
val user_brands_grouped = beauty.map(row=>(row._5,row._1)).distinct.groupByKey()
// filter rdd to only users who have commented on multiple brands' posts
val user_brands_grouped_multi = user_brands_grouped.filter(row=>row._2.size>1).map(row=>(row._1,row._2.toSet)).map(row=>(row._2,row._1))

// rdd of (brand, (users that commented on that brands' posts))
val user_brands = beauty.map(row=>(row._1, row._5)).distinct.groupByKey().map(row=>(row._1, row._2.toSet))

// list of distinct brands to use in get_union() function
val distinct_brands = beauty.map(row=>row._1).distinct.collect().toList
// map of (brand: (users who have commented on that brand's posts)) to use in get_union() function
val user_brands_map = beauty.map(row=>(row._1, row._5)).distinct.groupByKey().map(row=>(row._1, row._2.toSet)).collectAsMap

def get_union(brand: String, users1: scala.collection.immutable.Set[String]) = {
	var cross_counts = collection.mutable.Map[String, Int]()
	for (brand <- distinct_brands) {
		val overlap = users1.intersect(user_brands_map(brand)).size
		cross_counts += (brand -> overlap)
	}
	cross_counts
}


val user_brands_intersect = user_brands.map(row=>(row._1,get_union(row._1,row._2)))

// convert to dataframe
val df = user_brands_intersect.toDF("col1","col2")
// explode dataframe
val exploded = df.select(df("col1"),explode(df("col2")))
// convert to rdd for saving back to hdfs
val exploded_rdd = exploded.rdd.filter(row=>row(0)!=row(1))

// save to hdfs
user_brands_grouped_multi.saveAsTextFile("insta_cleaned/user_brands_grouped_multi")
exploded_rdd.saveAsTextFile("insta_cleaned/comment_overlap")