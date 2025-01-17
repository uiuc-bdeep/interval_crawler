'''
	File Name: data_loader.py
	Author: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Maintainer: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	
	Description:
		This script initializes the log, cleans the log and starts
		the Interval crawler every week. It calls the data loader and scheduler.

		The log file, original CSV File, and all temporary files are located at: 
		ubuntu@141.142.209.140://home/ubuntu/interval_crawler_vX/

		Old logs (every week) are shifted to:
		/data/Congestion/stream:/data/Congestion/stream/interval_crawler_vX/logs/

		The csv output is written to:
		/data/Congestion/stream:/data/Congestion/stream/interval_crawler_vX/

	Database Info:
		DB: trial
		Collection: try0
'''

# Import libraries.
import data_loader
import scheduler
import csv_writer
import os
import sys
import time
import datetime
import logging
from pymongo import MongoClient

# Set up database connection.
client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
db = client.trial

# Global variable to keep track of week number.
week_number = 5

#-----------------------------------------------------------------------#
#						Function: Interval Crawler Init					#
#-----------------------------------------------------------------------#
def interval_crawler_init():
	# Update week number and convert to string.
	global week_number
	week_number = week_number + 1
	week_parameter = str(week_number)

	# Call data_loader for the week.
	logger.info("Calling Data Loader For Week: "+week_parameter)
	data_loader.load_data(week_parameter)
	logger.info("Loaded Data for Week: "+week_parameter)

	# Call scheduler for the week.
	logger.info("Calling Scheduler for Week: "+week_parameter)
	scheduler.schedule_trips(week_parameter)
	logger.info("Crawled all trips for Week: "+week_parameter)

# Non-stop loop to start weekly crawls.
while True:

	# Scheduled start time for normal-crawler for every week.
	start_day = "Sunday"
	start_hour = "18"
	start_min = "15"

	# Calculate current time and check if current time = start time.
	now = datetime.datetime.now()
	current_day = now.strftime("%A")

	if start_day == current_day and start_hour==str(now.hour) and start_min==str(now.minute):
		# Create log.
		logger = logging.getLogger("interval_crawler")
		logger.setLevel(logging.DEBUG)

		# Create the log handler & reset every week.
		lh = logging.FileHandler("interval_crawler_log.txt", mode="w")

		# Format the log.
		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		lh.setFormatter(formatter)

		# Add handler to the logger object.
		logger.addHandler(lh)

		# Call normal crawler.
		logger.info("Starting Interval Crawler")
		print ("Starting Interval Crawler on "+str(now))
		interval_crawler_init()
		print ("Week " + str(week_number) + " Done.")
		logger.info("Week " + str(week_number) + " Done.")

		# Close log.
		logger.removeHandler(lh)

	# Check every 30 seconds.
	time.sleep(30)