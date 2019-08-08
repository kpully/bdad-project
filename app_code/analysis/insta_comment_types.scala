// comment types module

def define_comments(brand: String, comment:String, user: String): String = {
	if ((comment contains ("@"+brand)) && (comment contains "?")) {
		return "query"
	} else if (brand==user) {
		return "reply"
	} else if (comment.toLowerCase.contains("check out my")) {
		return "spam-promotion"
	} else if (comment.toLowerCase.contains("visit my")) {
		return "spam-promotion"
	} else if (comment.toLowerCase.contains("?")) {
		return "question-no-brand-mention"
	} else if (comment.toLowerCase.contains("you should make")) {
		return "brand-directed-statement"
	}  else if (comment.toLowerCase.contains("you should create")) {
		return "brand-directed-statement"
	}  else if (comment.toLowerCase.contains("why don't you")) {
		return "brand-directed-statement"
	}  else if (comment.toLowerCase.contains("@")) {
		return "mention"
	}  else if (comment.contains("#")) {
		return "hashtag"
	} else {
		return "misc"
	}
}

val comment_types = beauty.map(row=>(row._1,row._2, row._3,row._4,row._5,row._6,row._7,define_comments(row._1, row._7, row._5)))
val comment_types_filtered = comment_types.filter(row=>row._8!="misc")
comment_types_filtered.saveAsTextFile("insta_cleaned/comment_types")