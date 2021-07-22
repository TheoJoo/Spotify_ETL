import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = ""
TOKEN = ""

	# Generate your token here: https://developer.spotify.com/console/get-recently-played/

# Transformation step
def check_if_valid_data(df: pd.DataFrame) -> bool:
	# Check if dataframe is empty
	if df.empty:
		print("No songs downloaded. Finishing execution")
		return False

	# Primary Key
	if pd.Series(df['played_at']).is_unique:
		pass
	else:
		raise Exception("Primary Key Check is violated")

if __name__ == "__main__":

	# Extract step
	# Send some information in the header, according to the Spotify API
	headers = {
		"Accept": "appliaction/json",
		"Content-Type": "application/json",
		"Authorization": "Bearer {token}".format(token=TOKEN)
	}

	# We want to run this feed daily, we want the songs in the last 24hours
	today = datetime.datetime.now()
	yesterday = today - datetime.timedelta(days=1)
	# We need to convert to unix timestamp in milliseconds, according to the Spotify API
	yesterday_unix_timestamp = int(yesterday.timestamp())*1000

	# Number of tracks we want to extract, min=1, max=50.
	numberOfTracks = 50

	r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit={limit}&after={time}".format(limit=numberOfTracks, time=yesterday_unix_timestamp), headers = headers)

	data = r.json()

	song_names = []
	artist_names = []
	played_at_list = []
	timestamps = []

	for song in data["items"]:
		song_names.append(song["track"]["name"])
		artist_names.append(song["track"]["album"]["artists"][0]["name"])
		played_at_list.append(song["played_at"])
		timestamps.append(song["played_at"][0:10])

	song_dict = {
		"song_name" : song_names,
		"artist_name" : artist_names,
		"played_at" : played_at_list,
		"timestamp" : timestamps
	}

	song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

	print(song_df)