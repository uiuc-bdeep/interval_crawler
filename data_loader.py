'''
	File Name: data_loader.py
	Author: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Maintainer: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Description:
		This script parses data from the CSV Trip Survey and creates a random sample of 
		20 Percent and then creates a JSON file for each day of the week.
		A week number (string), city and survey year are added to the JSON Objects.
		A datestamp for every day of the week is also added.
		The JSON files are then uploaded to a MongoDB database.
'''

# Import libraries.
import os
import csv
import json
import random
import time
import datetime
import logging
import requests
from pymongo import MongoClient

#-----------------------------------------------------------------------#
#							Function: Load Data							#
#-----------------------------------------------------------------------#
def load_data(week_number):
	# Open Log and log date.
	logger = logging.getLogger("interval_crawler.data_loader")
	logger.info("Loading data for week: "+str(week_number))

	# Set up database connection.
	client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
	db = client.trial
	record = db.try0

	# Create datestamps for trips.
	current_date = datetime.datetime.today().strftime('%Y/%m/%d')
	current_date_str = datetime.datetime.strptime(current_date, '%Y/%m/%d')
	week_date = current_date_str + datetime.timedelta(days=+1)
	monday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	week_date = current_date_str + datetime.timedelta(days=+2)
	tuesday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	week_date = current_date_str + datetime.timedelta(days=+3)
	wednesday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	week_date = current_date_str + datetime.timedelta(days=+4)
	thursday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	week_date = current_date_str + datetime.timedelta(days=+5)
	friday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)

	monday_list = []
	page = open("monday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		monday_list.append(item)
	page.close()

	monday_length = len(monday_list)

	for num in range(0,monday_length):
		monday_list[num]["weeks"] = week_number
		monday_list[num]["timestamp"]["week"] = monday

	body_monday_items = json.dumps(monday_list, sort_keys = True, indent = 4, separators = (',',':'))
	f = open('monday.json', 'w')  
	f.write(body_monday_items)
	f.close()

	tuesday_list = []
	page = open("tuesday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		tuesday_list.append(item)
	page.close()

	tuesday_length = len(tuesday_list)

	for num in range(0,tuesday_length):
		tuesday_list[num]["weeks"] = week_number
		tuesday_list[num]["timestamp"]["week"] = tuesday

	body_tuesday_items = json.dumps(tuesday_list, sort_keys = True, indent = 4, separators = (',',':'))
	f = open('tuesday.json', 'w')  
	f.write(body_tuesday_items)
	f.close()

	wednesday_list = []
	page = open("wednesday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		wednesday_list.append(item)
	page.close()

	wednesday_length = len(wednesday_list)

	for num in range(0,wednesday_length):
		wednesday_list[num]["weeks"] = week_number
		wednesday_list[num]["timestamp"]["week"] = wednesday

	body_wednesday_items = json.dumps(wednesday_list, sort_keys = True, indent = 4, separators = (',',':'))
	f = open('wednesday.json', 'w')  
	f.write(body_wednesday_items)
	f.close()

	thursday_list = []
	page = open("thursday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		thursday_list.append(item)
	page.close()

	thursday_length = len(thursday_list)

	for num in range(0,thursday_length):
		thursday_list[num]["weeks"] = week_number
		thursday_list[num]["timestamp"]["week"] = thursday

	body_thursday_items = json.dumps(thursday_list, sort_keys = True, indent = 4, separators = (',',':'))
	f = open('thursday.json', 'w')  
	f.write(body_thursday_items)
	f.close()

	friday_list = []
	page = open("836percent_friday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		friday_list.append(item)
	page.close()

	friday_length = len(friday_list)

	for num in range(0,friday_length):
		friday_list[num]["weeks"] = week_number
		friday_list[num]["timestamp"]["week"] = friday

	body_friday_items = json.dumps(friday_list, sort_keys = True, indent = 4, separators = (',',':'))
	f = open('836percent_friday.json', 'w')  
	f.write(body_friday_items)
	f.close()

	# Push JSON Objects from the file into the database.	
	page = open("monday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		record.insert(item)
	page.close()
	page = open("tuesday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		record.insert(item)
	page.close()
	page = open("wednesday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		record.insert(item)
	page.close()
	page = open("thursday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		record.insert(item)
	page.close()
	page = open("836percent_friday.json", 'r')
	parsed = json.loads(page.read())
	for item in parsed:
		record.insert(item)
	page.close()
	logger.info("Loaded data into the database.")

	# Send notification to Slack.
	url = "https://hooks.slack.com/services/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"
	data_loader_msg = "Sao Paulo 2012 Survey Interval-Crawler: Data loading succesful."
	payload={"text": data_loader_msg}
	try:
		r = requests.post(url, data=json.dumps(payload))
	except requests.exceptions.RequestException as e:
		logger.info("Sao Paulo 2012 Survey Interval-Crawler: Error while sending data loader Slack notification.")
		logger.info(e)
		logger.info(data_loader_msg)

	total_length = monday_length + tuesday_length + wednesday_length + thursday_length + friday_length
	logger.info(total_length)
	tl = str(total_length)

	url = "https://hooks.slack.com/services/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"
	data_loader_msg = "Total Number of trips: ."+tl
	payload={"text": data_loader_msg}
	try:
		r = requests.post(url, data=json.dumps(payload))
	except requests.exceptions.RequestException as e:
		logger.info("Sao Paulo 2012 Survey Interval-Crawler: Error while sending data loader Slack notification.")
		logger.info(e)
		logger.info(data_loader_msg)