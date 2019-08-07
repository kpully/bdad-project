import pandas as pd
import os
import json


def main():
	base_path = "instagram_data/"
	EXCLUDE = [".DS_Store", "instagram-scraper.log"]
	subdirs = [x for x in os.listdir(base_path) if x not in EXCLUDE]
	filepaths = [base_path+x+"/"+x+".json" for x in subdirs]


	post_owner_all, post_ts_all, post_url_all, comment_ts_all, comment_id_all, comment_owner_username_all,comment_owner_id_all,comment_text_all = [],[],[],[],[],[],[],[]

	for path in filepaths:
	    with open(path, "r") as read_file:
	        data = json.load(read_file)
	        
	        posts = data["GraphImages"]
	    for post in posts:
	        post_details = json.loads(json.dumps(post))

	        post_owner = post_details["username"]
	        post_ts = post_details["taken_at_timestamp"]
	        post_url = post_details["urls"]

	        post_comments = json.loads(json.dumps(post_details["comments"]))
	        post_comments_data = json.loads(json.dumps(post_comments["data"]))

	        for comment in post_comments_data:
	            comment_ts = comment["created_at"]
	            comment_id = comment["id"]
	            comment_owner = json.loads(json.dumps(comment["owner"]))
	            comment_owner_username = comment_owner["username"]
	            comment_owner_id = comment_owner["id"]
	            comment_text = comment["text"]

	            post_owner_all.append(post_owner)
	            post_ts_all.append(post_ts)
	            post_url_all.append(post_url)
	            comment_ts_all.append(comment_ts)
	            comment_id_all.append(comment_id)
	            comment_owner_username_all.append(comment_owner_username)
	            comment_owner_id_all.append(comment_owner_id)
	            comment_text_all.append(comment_text)

if__name__== "__main__" :
	main()