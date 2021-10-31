"""This python script scrapes data from steamcharts.com and its subpages to load into a PostGresSQL database. FOR DEMO PURPOSES ONLY"""
import requests # Used to fetch HTML
from bs4 import BeautifulSoup # Used to parse HTML
import pandas as pd # Used for data manipulation
from sqlalchemy import create_engine # Used to connect to PostgreSQL
import re # Used for string manipulation
import time # Used for waits
TOP_GAMES_HEAD = 'https://steamcharts.com/top/'
APP_URL = 'https://steamcharts.com/app/'
PAGES = 20 # 20 pages for 500
all_tables = [] 
app_id_table = []
player_count_table = []
def get_page_table(html): 
	page_table = html.find("table", {"id":"top-games"})
	return page_table
def get_id(table):
	app_ids = []
	a_tags = table.find_all('a',href = True)
	for a in a_tags:
		href = a['href']
		href=re.sub("[^0-9]", "", href) # keep only numbers
		app_ids.append(href)
	return app_ids
def get_monthly_players(html):
	player_counts = []
	monthly_player_counts = html.find("td", {"class":"right num-f italic"})
	monthly_player_counts = monthly_player_counts.getText()
	monthly_player_counts = monthly_player_counts[:-3] # Truncate last three to remove decimals
	player_counts.append(monthly_player_counts)
	return player_counts
for page in range(1,PAGES+1): # Loop through each page, parsing html, getting the main table from each page
	url = TOP_GAMES_HEAD + 'p.' + str(page)
	raw_html = requests.get(url)
	parsed_html = BeautifulSoup(raw_html.content, 'html.parser')
	page_table = get_page_table(parsed_html)
	app_id_table += get_id(page_table)
	page_table = str(page_table)
	all_tables.append(pd.read_html(page_table)[0])
	time.sleep(3) # 2 Seconds to lower resources used by page requests
	print(f'page {page} of {PAGES}')
for app_id in range(len(app_id_table)): # Loop through each app id and scrape its individual page for 30-day player count
	url = APP_URL + app_id_table[app_id]
	raw_html = requests.get(url)
	parsed_html = BeautifulSoup(raw_html.content, 'html.parser')
	player_count_table += get_monthly_players(parsed_html)
	time.sleep(3)
	print(f'page {app_id+1} of {len(app_id_table)}')
dataframe = pd.concat(all_tables) # Create dataframe of all table data
dataframe['APP_ID'] = app_id_table 
dataframe['30 Day Average'] = player_count_table
dataframe['30 Day Average'] = dataframe['30 Day Average'].astype(int)
dataframe['Peak Players'] = dataframe['Peak Players'].astype(int)
dataframe['Hours Played'] = dataframe['Hours Played'].astype(int)
dataframe=dataframe.drop('Last 30 Days',1) # Drop NAN column
dataframe=dataframe.drop('Current Players',1)# Drop irrevalant column
print("##############")
engine = create_engine('postgresql://user:pass@localhost:port/database') # connect to postgres database
dataframe.to_sql('SteamCharts', engine) # create table and insert dataframe
input("Complete")
