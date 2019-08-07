## install rtweet from CRAN
#install.packages("rtweet")
#install.packages("httpuv")

## load rtweet package
library(rtweet)

## install remotes package if it's not already
if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes")
}

## install httpuv if not already
if (!requireNamespace("httpuv", quietly = TRUE)) {
  install.packages("httpuv")
}

# create token named "twitter_token"
token <- create_token(
  app = "NYU-RBDA",
  consumer_key = "o5cRvdVIRHd7WIS0ZbGTcu0UV",
  consumer_secret = "BJkOGi1COMFUgOrIs3S7jjvsGSoH8dmyRiUuUu5gMlgl1Wh4ps",
  access_token = "2817235339-pAa7y1o1d4nNPmzM9CxKy2c4s9e6ZbZoUOpLjGV",
  access_secret = "fh4L5lnMMLAeHbc5ahqu5ZDOgXRnXm6ISpuD2Huz8EjEo")

identical(token, get_token())

get_brand_tweets <- function(brand,numTweets) {
  tryCatch({
    
  ## search for 500 tweets using the #glossier hashtag
  data_tw <- search_tweets(q = brand, n = numTweets, lang="en",include_rts = FALSE)
  
  # view the first 3 rows of the dataframe
  head(data_tw, n = 3)
  
  # exclude list columns (irrelevant and throws errors)
  cleaned_data <- data_tw[, sapply(data_tw, class) != "list"]
  
  #bind
  TW_data <- cbind(Brand=brand,cleaned_data)
  
  #write to csv
  write.table(TW_data, "TW_data.csv", sep = ",", row.names=FALSE, na="", col.names = !file.exists("TW_data.csv"), append = T)
  
  }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
}

fileName <- "brand_hashtags.txt"
conn <- file(fileName,open="r")
linn <-readLines(conn,warn=FALSE)
for (i in 1:length(linn)){
  get_brand_tweets(linn[i],2000)
}
close(conn)


    