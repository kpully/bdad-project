// top commenters module

// rdd of ((brand,user),#comments) filtered to at least 10 comments per user per brand and excluding brands commenting on their own posts
val top_commenters = beauty.map(row=>((row._1,row._5),1)).reduceByKey((v1,v2)=>v1+v2).filter(row=>row._2>10).filter(row=>row._1._1!=row._1._2)

//rdd of ((brand,username),comment)
val user_comments = beauty.map(row=>((row._1, row._5),row._7))

//clean top_commenters to create rdd of (brand,user,comment)
val top_commenters_comments = top_commenters.join(user_comments).map(row=>(row._1._1, row._1._2, row._2._2))

// save files
top_commenters.saveAsTextFile("insta_cleaned/top_commenters")
top_commenters_comments.saveAsTextFile("insta_cleaned/top_commenters_comments")