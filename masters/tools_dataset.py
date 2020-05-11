"""
========================= !!! READ ME !!! =========================
This script contains definitons of functions for work with datasets.
Make sure you have installed all requirements from requirements.txt
===================================================================
"""

# Libraries: Import global
import requests
import json
import time

# Libraries: Import local
from masters.tools_system import makedir
from masters.paths import path_datasets, path_dataset_lastfm2020, path_dataset_mtt

# Function: Build MagnaTagATune Dataset folder structure
def folderstruct_mtt_dataset():
    makedir(path_datasets)
    makedir(path_dataset_mtt)
    makedir(path_dataset_mtt + 'tracks_wav')
    makedir(path_dataset_mtt + 'tracks_mp3')
    makedir(path_dataset_mtt + 'features_melgram')

# Function: Build Last.fm Dataset 2020 folder structure
def folderstruct_lastfm_dataset():
    makedir(path_datasets)
    makedir(path_dataset_lastfm2020)
    makedir(path_dataset_lastfm2020 + 'tracks_wav')
    makedir(path_dataset_lastfm2020 + 'tracks_mp3')
    makedir(path_dataset_lastfm2020 + 'features_melgram')

# Function: Last.fm API GET request
def get_lastfm(parameters_input, api_key_lastfm):
    # Variabls: Set local
    user_agent_lastfm = 'Masters'
    api_url_lastfm = 'http://ws.audioscrobbler.com/2.0/'
    # Last.fm API header and default parameters
    headers = {
        'user-agent': user_agent_lastfm
    }
    parameters = {
        'api_key': api_key_lastfm,
        'format': 'json'
    }
    parameters.update(parameters_input)
    # Responses and error codes
    state = False
    while state == False:
        try:
            response = requests.get(api_url_lastfm, headers=headers, params=parameters, timeout=10)
            if response.status_code == 200: 
                print('Last.fm API: 200 - Response was successfully received.')
                state = True
            elif response.status_code == 401:
                print('Last.fm API: 401 - Unauthorized. Please check your API key.')
                exit()
            elif response.status_code == 429:
                print('Last.fm API: 429 - Too many requests. Waiting 60 seconds.')
                time.sleep(60)
                state = False
            else:
                print('Last.fm API: Unspecified error. No response was received. Trying again after 60 seconds...')
                time.sleep(60)
                state = False
        except OSError as err:
            print('Error: ' + str(err))
            print('Trying again...')
            time.sleep(3)
            state = False
    return response.json()

# Function: Spotify API GET request
def get_spotify(id_spotify, api_key_spotify):
    # Variables: Set local
    api_url_spotify = 'https://api.spotify.com/v1/tracks/'
    # Last.fm API header
    headers = {
        'Authorization': 'Bearer ' + api_key_spotify,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    url = api_url_spotify + id_spotify + '?market=CZ'
    # Responses and error codes
    state = False
    while state == False:
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200: 
                print('Spotify API: 200 - Response was successfully received.')
                time.sleep(0.01)
                state = True
            elif response.status_code == 401:
                print('Spotify API: 401 - Unauthorized. Please check your API key.')
                exit()
            elif response.status_code == 429:
                print('Spotify API: 429 - Too many requests. Waiting 60 seconds.')
                time.sleep(60)
                state = False
            else:
                print('Spotify API: Unspecified error. No response was received. Trying again after 60 seconds...')
                time.sleep(60)
                state = False
        except OSError as err:
            print('Error: ' + str(err))
            print('Trying again...')
            time.sleep(3)
            state = False
    return response.json()

# Function: Delete track from dataset
def dataset_delete(connection, cursor, id_dataset):
    cursor.execute('DELETE FROM metadata WHERE "id_dataset" = "' + id_dataset + '"')
    connection.commit()
    cursor.execute('DELETE FROM tags WHERE "id_dataset" = "' + id_dataset + '"')
    connection.commit()

# Function: Definition of json formatting (nicer print command)
def json_format(response_json):
    text = json.dumps(response_json, sort_keys=True, indent=4)
    return text