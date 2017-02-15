'''
	File Name: crawler.py
	Author: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Maintainer: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Description:	
		This script pulls trips from the database, checks for errors, write trips to a JSON
		file and then output to a CSV file.
'''
# Import libraries.
import os
import csv
import json
import requests
import logging
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps

#-----------------------------------------------------------------------#
#							Function: Make CSV 							#
#-----------------------------------------------------------------------#
def make_csv(week,day):
	# Open Log and add details.
	logger = logging.getLogger("interval_crawler.csv_writer")
	logger.info("Creating CSV file for week: "+str(week)+"day "+str(day))

	# Create file name.
	MAIN_NAME = "interval-crawler-"
	INCREMENTAL_FILENAME_SUFFIX = str(week)+"-"+str(day)
	NAME_EXTENSION = ".csv"
	OUTPUT_DIR = "/data/Congestion/stream/interval-crawler/"
	FINAL_NAME = OUTPUT_DIR+MAIN_NAME+INCREMENTAL_FILENAME_SUFFIX+NAME_EXTENSION

	# Set up database connection.
	client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
	db = client.trial

	# Find all trips for the given day of the week.
	r = db.try0.find({"weeks":week,"timestamp.day":day})
	l = list(r)
	length = len(l)
	logger.info("Number of trips to write to csv today: "+str(length))

	# Initialize number of error crawls.
	error_crawls = 0

	# Loop through all trips to replace with blanks for errors.
	for num in range(length):
		if str(l[num]["time_true"]["distance"]) == "-3" or str(l[num]["time_true"]["traffic"]) == "-3" or str(l[num]["time_true"]["distance"]) == "-2" or str(l[num]["time_true"]["traffic"]) == "-2" or str(l[num]["time_true"]["distance"]) == "-1" or str(l[num]["time_true"]["traffic"]) == "-1":
			error_crawls = error_crawls + 1
			l[num]["time_true"]["distance"] = " "
			l[num]["time_true"]["traffic"] = " "
			l[num]["time_true"]["time"] = " "
		if str(l[num]["time_500"]["distance"]) == "-3" or str(l[num]["time_500"]["traffic"]) == "-3" or str(l[num]["time_500"]["distance"]) == "-2" or str(l[num]["time_500"]["traffic"]) == "-2" or str(l[num]["time_500"]["distance"]) == "-1" or str(l[num]["time_500"]["traffic"]) == "-1":
			error_crawls = error_crawls + 1
			l[num]["time_500"]["distance"] = " "
			l[num]["time_500"]["traffic"] = " "
			l[num]["time_500"]["time"] = " "
		if str(l[num]["time_700"]["distance"]) == "-3" or str(l[num]["time_700"]["traffic"]) == "-3" or str(l[num]["time_700"]["distance"]) == "-2" or str(l[num]["time_700"]["traffic"]) == "-2" or str(l[num]["time_700"]["distance"]) == "-1" or str(l[num]["time_700"]["traffic"]) == "-1":
			error_crawls = error_crawls + 1
			l[num]["time_700"]["distance"] = " "
			l[num]["time_700"]["traffic"] = " "
			l[num]["time_700"]["time"] = " "
		if str(l[num]["time_1000"]["distance"]) == "-3" or str(l[num]["time_1000"]["traffic"]) == "-3" or str(l[num]["time_1000"]["distance"]) == "-2" or str(l[num]["time_1000"]["traffic"]) == "-2" or str(l[num]["time_1000"]["distance"]) == "-1" or str(l[num]["time_1000"]["traffic"]) == "-1":
			error_crawls = error_crawls + 1
			l[num]["time_1000"]["distance"] = " "
			l[num]["time_1000"]["traffic"] = " "
			l[num]["time_1000"]["time"] = " "
		if str(l[num]["time_1400"]["distance"]) == "-3" or str(l[num]["time_1400"]["traffic"]) == "-3" or str(l[num]["time_1400"]["distance"]) == "-2" or str(l[num]["time_1400"]["traffic"]) == "-2" or str(l[num]["time_1400"]["distance"]) == "-1" or str(l[num]["time_1400"]["traffic"]) == "-1":
			error_crawls = error_crawls + 1
			l[num]["time_1400"]["distance"] = " "
			l[num]["time_1400"]["traffic"] = " "
			l[num]["time_1400"]["time"] = " "
		if str(l[num]["time_1830"]["distance"]) == "-3" or str(l[num]["time_1830"]["traffic"]) == "-3" or str(l[num]["time_1830"]["distance"]) == "-2" or str(l[num]["time_1830"]["traffic"]) == "-2" or str(l[num]["time_1830"]["distance"]) == "-1" or str(l[num]["time_1830"]["traffic"]) == "-1":
			error_crawls = error_crawls + 1
			l[num]["time_1830"]["distance"] = " "
			l[num]["time_1830"]["traffic"] = " "
			l[num]["time_1830"]["time"] = " "
		if str(l[num]["time_2200"]["distance"]) == "-3" or str(l[num]["time_2200"]["traffic"]) == "-3" or str(l[num]["time_2200"]["distance"]) == "-2" or str(l[num]["time_2200"]["traffic"]) == "-2" or str(l[num]["time_2200"]["distance"]) == "-1" or str(l[num]["time_2200"]["traffic"]) == "-1":
			error_crawls = error_crawls + 1
			l[num]["time_2200"]["distance"] = " "
			l[num]["time_2200"]["traffic"] = " "
			l[num]["time_2200"]["time"] = " "

	# Send number of errors to slack.
	url = "https://hooks.slack.com/services/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"
	err_msg = "Sao Paulo 2012 Survey Interval-Crawler: There were " + str(error_crawls) + " errors on week-"+str(week)+"-day-"+str(day)+"."
	payload1={"text": err_msg}
	try:
		r = requests.post(url, data=json.dumps(payload1))
		logger.info(str(err_msg))
	except requests.exceptions.RequestException as e:
		logger.info("Error while sending Slack Notification 1")
		logger.info(str(e))
		logger.info(str(err_msg))

	ljson = dumps(l,sort_keys = True, indent = 4, separators = (',',':'))
	f = open('json_from_db.json', 'w')
	f.write(ljson)
	f.close()

	file = open('json_from_db.json','r')
	x = json.loads(file.read())

	f = open(FINAL_NAME, "ab+")
	z = csv.writer(f)

	z.writerow(["city","survey","trip_id","weeks","origin_latitude","origin_longitude","destination_latitude","destination_longitude","timestamp_week","timestamp_day","timestamp_hours","timestamp_minutes","distance_500","time_500","traffic_500","distance_700","time_700","traffic_700","distance_1000","time_1000","traffic_1000","distance_1400","time_1400","traffic_1400","distance_1830","time_1830","traffic_1830","distance_2200","time_2200","traffic_2200"])
	for index in x:
		z.writerow([index["city"],index["survey"],index["trip_id"],index["weeks"],index["origin"]["latitude"],index["origin"]["longitude"],index["destination"]["latitude"],index["destination"]["longitude"],index["timestamp"]["week"],index["timestamp"]["day"],index["timestamp"]["hours"],index["timestamp"]["minutes"],index["time_500"]["distance"],index["time_500"]["time"],index["time_500"]["traffic"],index["time_700"]["distance"],index["time_700"]["time"],index["time_700"]["traffic"],index["time_1000"]["distance"],index["time_1000"]["time"],index["time_1000"]["traffic"],index["time_1400"]["distance"],index["time_1400"]["time"],index["time_1400"]["traffic"],index["time_1830"]["distance"],index["time_1830"]["time"],index["time_1830"]["traffic"],index["time_2200"]["distance"],index["time_2200"]["time"],index["time_2200"]["traffic"]])
	f.close()

	print ("Done. CSV File Created for Week: "+str(week)+" Day: "+str(day))

	# Send Slack notification after successfully writing CSV file.
	url = "https://hooks.slack.com/services/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"
	csv_msg = "Sao Paulo 2012 Survey Interval-Crawler: CSV for week-"+str(week)+"-day-"+str(day)+" has been written successfully to the shared drive."
	payload1={"text": csv_msg}
	try:
		r = requests.post(url, data=json.dumps(payload1))
	except requests.exceptions.RequestException as e:
		logger.info(str("Error while sending Slack Notification 2"))
		logger.info(str(e))
		logger.info(str(csv_msg))
