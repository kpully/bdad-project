// queries module

def is_query(brand: String, comment: String) = { ((comment contains ("@"+brand)) && (comment contains "?"))}

// get type of query
def is_query_detailed(brand: String, comment: String): String = { 
	if ((comment contains ("@"+brand)) && (comment contains "?")) {
		if (comment.toLowerCase contains "location") {
			return "query-location"
		} else if (comment.toLowerCase.contains("where")) {
			return "query-location"
		} else if (comment.toLowerCase.contains("in store")) {
			return "query-location"
		} else if (comment.toLowerCase.contains("ship to")) {
			return "query-location"
		} else if (comment.toLowerCase.contains("store in ")) {
			return "query-location"
		} else if (comment.toLowerCase.contains("available in ")) {
			return "query-location"
		} else if (comment.toLowerCase.contains("germany")) {
			return "query-location"
		} else if (comment.toLowerCase.contains("netherlands")) {
			return "query-location"
		} else if (comment.toLowerCase.contains("japan")) {
			return "query-location"
		} else if (comment.toLowerCase.contains("canada")) {
			return "query-location"
		} else if (comment.toLowerCase.contains(" uk ")) {
			return "query-location"
		} else if (comment.toLowerCase contains "australia") {
			return "query-location"
		} else if (comment.toLowerCase.contains("ship to ")) {
			return "query-location"
		} else if (comment.toLowerCase.contains("coming to ")) {
			return "query-location"
		} else if (comment.toLowerCase contains "new york") {
			return "query-location"
		} else if (comment.toLowerCase.contains("italy")) {
			return "query-location"
		} else if (comment.toLowerCase.contains("india")) {
			return "query-location"
		}  else if (comment.toLowerCase.contains("europe")) {
			return "query-location"
		} else if (comment.toLowerCase.contains("when")) {
			return "query-when"
		} else if (comment.toLowerCase.contains("why")) {
			return "query-why"
		} else if (comment.toLowerCase.contains("what")) {
			return "query-what"
		} else if (comment.toLowerCase.contains("how")) {
			return "query-how"
		} else if (comment.toLowerCase.contains("which")) {
			return "query-which"
		} else if (comment.toLowerCase.contains("does this")) {
			return "query-product-detail"
		} else if (comment.toLowerCase.contains("does it")) {
			return "query-product-detail"
		} else if (comment.toLowerCase.contains("is it ")) {
			return "query-product-detail"
		} else if (comment.toLowerCase.contains("is this ")) {
			return "query-product-detail"
		} else if (comment.toLowerCase.contains("will it ")) {
			return "query-product-detail"
		} else if (comment.toLowerCase.contains("will this ")) {
			return "query-product-detail"
		} else if (comment.toLowerCase.contains("can it ")) {
			return "query-product-detail"
		} else if (comment.toLowerCase.contains("can the ")) {
			return "query-product-detail"
		} else if (comment.toLowerCase.contains("is the ")) {
			return "query-product-detail"
		} else if (comment.toLowerCase.contains("are the ")) {
			return "query-product-detail"
		} else if (comment.toLowerCase.contains("does the ")) {
			return "query-product-detail"
		}  else if (comment.toLowerCase.contains("are they ")) {
			return "query-product-detail"
		} else if (comment.toLowerCase.contains("can this ")) {
			return "query-product-detail"
		} else if (comment.toLowerCase.contains("restock")) {
			return "query-stock"
		} else if (comment.toLowerCase.contains("in stock")) {
			return "query-stock"
		} else if (comment.toLowerCase.contains("discontinue")) {
			return "query-stock"
		}
		else {
			return "misc"
		}
	} 
	else {
		return "not_query"
	}
}

// rdd of (brand, # of comments)
def comment_count_by_brand = beauty.map(line=>(line._1,1)).reduceByKey((v1,v2)=>v1+v2)

// filter comments to only queries
val queries = beauty.filter(row=>is_query(row._1,row._6)==true)

// run queries through detailed query method to get query type
val queries_detailed = beauty.map(row=>(row._1, row._2, row._3, row._4, row._5, row._6, row._7,is_query_detailed(row._1, row._7))).filter(row=>row._8!="not_query")

val queries_by_brand = queries.map(line=>(line._1,1)).reduceByKey((v1,v2)=>v1+v2)

// save to hdfs
queries.saveAsTextFile("insta_cleaned/queries")
queries_detailed.saveAsTextFile("insta_cleaned/queries_detailed")