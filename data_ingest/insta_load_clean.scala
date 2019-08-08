// load instagram data
val insta = sc.textFile("instagram_comments.csv").map(line=>line.split(",")).filter(line=>line.length==7)
val beauty = clean.map(row=>(row(0),row(1), row(2), row(3),row(4),row(5),row(6))).distinct