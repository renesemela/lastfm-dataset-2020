"""
========================= !!! READ ME !!! =========================
Use this command to get script description
    python dataset_lastfm.py --help
Make sure you have installed all requirements from requirements.txt
===================================================================
"""

# Libraries: Import global
import argparse

# Variables: Set global (can be edited)
api_key_lastfm = ''
api_key_spotify = ''
n_tags = 107 # 107 is because some synonymous and unrelevant tags are present in top 100 Last.fm tags, so the final count is 100 after merging and deleting these tags.
n_tracks = 3000

# Function: Build dataset database
def build_db(n_tags, n_tracks):
    # Libraries: Import local
    import numpy as np
    import pandas as pd
    import sqlite3
    import re
    from hashlib import md5
    from math import ceil

    # Libraries: Import custom
    from masters.paths import path_dataset_lastfm2020, path_dataset_lastfm2020_db
    from masters.tools_dataset import folderstruct_lastfm_dataset, get_lastfm

    # Check if required folder structure is present
    print('Checking required folder structure.')
    folderstruct_lastfm_dataset()

    # Variables: Set local (can be experimentally edited)
    limit = 50
    n_pages = ceil(n_tracks/limit)

    # Last.fm API: Get 'n_tags' most popular tags
    print('\nGet top ' + str(n_tags) + ' tags.')
    toptags_json = get_lastfm({
        'method': 'chart.getTopTags',
        'limit': n_tags
        }, api_key_lastfm)
    toptags_list = []
    for i in range(n_tags):
        tag_current = toptags_json['tags']['tag'][i]['name']
        toptags_list.append(tag_current)

    # Prepare pd.DataFrame for dataset database data
    columns_names = toptags_list + ['id_dataset', 'id_spotify', 'url_spotify_preview', 'url_lastfm', 'artist', 'name']
    annotations = pd.DataFrame(0, index=np.arange((n_tags*n_tracks)+100000), columns=columns_names, dtype='object') # pd.DataFrame size workaround for unexpected behavior of Last.fm API

    # Last.fm API: For every tag from 'toptags_list' get 'n_tracks'
    index = 0
    for i in range(n_tags):
        print('\nGet top tracks for tag ' + str(i+1) + '/' + str(n_tags))
        for j in range(n_pages):
            print('Page ' + str(j+1) + '/' + str(n_pages))
            tracks_tag_json = get_lastfm({
                'method': 'tag.getTopTracks',
                'tag': toptags_list[i],
                'limit': limit,
                'page': j+1
                }, api_key_lastfm)
            for k in range(len(tracks_tag_json['tracks']['track'])):
                annotations.at[index, 'id_dataset'] = md5(tracks_tag_json['tracks']['track'][k]['url'].encode()).hexdigest() # MD5 hexadecimal hash of Last.fm URL used as dataset ID (batter pairing in future)
                annotations.at[index, 'id_spotify'] = 'None'
                annotations.at[index, 'url_spotify_preview'] = 'None'
                annotations.at[index, 'url_lastfm'] = tracks_tag_json['tracks']['track'][k]['url']
                annotations.at[index, 'artist'] = tracks_tag_json['tracks']['track'][k]['artist']['name']
                annotations.at[index, 'name'] = tracks_tag_json['tracks']['track'][k]['name']
                annotations.at[index, :n_tags] = 0
                annotations.at[index, toptags_list[i]] = 1
                index = index + 1

    # Remove zero rows
    annotations = annotations.loc[~(annotations==0).all(axis=1)]

    # Find duplicate rows (same tracks for multiple tags)
    print('\nRemoving duplicate rows. It may take a while. Be patient.')
    dup1 = annotations.duplicated(subset='id_dataset', keep = 'first')
    dup2 = annotations.duplicated(subset='id_dataset', keep = False)
    dup_list_orig = []
    for i in range(len(dup1)):
        if dup1[i] != dup2[i]:
            dup_list_orig.append(True)
        else:
            dup_list_orig.append(False)
    list_ids = annotations['id_dataset'].astype(str)
    # Go track by track
    del_index = []
    for i in range(len(list_ids)):
        print('\nRemoving duplicate rows for track ' + str(i+1) + '/' + str(len(list_ids)))
        # If duplicate is found find indexes of all occurrences
        if dup_list_orig[i] == True:
            id_current = list_ids[i]
            find_id_string = annotations['id_dataset'].str.find(id_current, start=0, end=len(id_current))
            dup_index = find_id_string[find_id_string == 0]
            dup_index = dup_index.index.values.tolist()
            # For each occurrence merge their tags and use first occurrence as result
            if len(dup_index) > 1:
                dup_list = annotations.iloc[dup_index]
                dup_list = dup_list.drop(['id_dataset', 'id_spotify', 'url_spotify_preview', 'url_lastfm', 'artist', 'name'], axis=1)
                dup_merge = pd.DataFrame(dup_list.sum(axis=0)).transpose()
                dup_merge = dup_merge.replace(list(range(1,10000)), 1)
                annotations.at[dup_index[0], list(dup_merge.columns)] = dup_merge.loc[0]
                del_index = del_index + dup_index[1:]
    # Finally remove all duplicates
    annotations = annotations.drop(del_index)

    # Make separate metadata
    annotations = annotations.reset_index(drop=True)
    metadata = annotations[['id_dataset', 'id_spotify', 'url_spotify_preview', 'url_lastfm', 'artist', 'name']]
    annotations = annotations.drop(['id_dataset', 'id_spotify', 'url_spotify_preview', 'url_lastfm', 'artist', 'name'], axis=1)

    # For each track find top tags and update dataset
    for i in range(len(metadata)):
        print('\nGet top tags for track ' + str(i+1) + '/' + str(len(metadata)))
        track_tags_json = get_lastfm({
            'method': 'track.getTopTags',
            'artist': metadata['artist'][i],
            'track': metadata['name'][i]
            }, api_key_lastfm)
        if 'toptags' in track_tags_json:
            track_tags_list = pd.DataFrame(track_tags_json['toptags']['tag'])
            for j in range(len(track_tags_list)):
                if track_tags_list['count'][j] >= 50 and track_tags_list['name'][j] in annotations.columns:
                    annotations.at[i, track_tags_list['name'][j]] = 1
        else:
            print('Error with getting top tags. Skipping this track.')

    # Merge synonymous tags (list)
    tags_synonyms = [
        ['hip hop', 'Hip-Hop', 'hiphop'],
        ['electronic', 'electronica'],
        ['female vocalists', 'female vocalist']]
    # Merge synonymous tags (tricky loops)
    for tags_synonyms_current in tags_synonyms:
        tags_synonyms_drop_list = []
        # Check which tags synonyms are present in current 'annotations'
        for i in range(len(tags_synonyms_current)):
            if tags_synonyms_current[i] in annotations.columns:
                tags_synonyms_drop_list.append(tags_synonyms_current[i])
        # Merge if synonyms are present
        if len(tags_synonyms_drop_list) > 1:
            annotations[tags_synonyms_drop_list[0]] = annotations[tags_synonyms_drop_list].max(axis=1)
            annotations = annotations.drop(tags_synonyms_drop_list[1:], axis=1)

    # Remove unrelevant tags (list)
    tags_unrelevant = [
        'albums I own',
        'Awesome',
        'under 2000 listeners',
        'seen live',
        'favorites',
        ]
    # Remove unrelevant tags
    for tags_unrelevant_current in tags_unrelevant:
        if tags_unrelevant_current in annotations.columns:
            annotations = annotations.drop(tags_unrelevant_current, axis=1)

    # Remove uppercase letters and spaces in tags names
    toptags_list_cleaned = list(annotations.columns)
    toptags_list_cleaned = [x.lower() for x in toptags_list_cleaned] # Make all tags lowercase
    # Remove spaces and '-' and replace with '_'
    for i in range(len(toptags_list_cleaned)):
        tag_current_split = re.split(' |-', toptags_list_cleaned[i])
        if len(tag_current_split) > 1:
            temp_string = tag_current_split[0]
            for j in range(1, len(tag_current_split)):
                temp_string = temp_string + '_' + tag_current_split[j]
                toptags_list_cleaned[i] = temp_string
    # Update annotation columns with new ones
    annotations.columns = toptags_list_cleaned

    # Change dtype of annotations to integer before inserting to DB
    annotations = annotations.astype('int8')

    # Make histogram of tags in dataset
    tags_hist = annotations.sum(axis=0)
    tags_hist = tags_hist.sort_values(axis=0, ascending=False) # Sort frome the most common tag to the least common
    print('\nTags density in dataset: ')
    print(tags_hist)

    # Put back metadata to annotations
    annotations['id_dataset'] = metadata['id_dataset']

    # Prepare tags names for SQL command
    list_edited = ['"' + x + '", ' for x in annotations.columns]
    tags_sql_string = ''.join(list_edited)
    tags_sql_string = tags_sql_string[:len(tags_sql_string)-2]

    # Prepare metadata names for SQL command
    list_edited = ['"' + x + '", ' for x in metadata.columns]
    metadata_sql_string = ''.join(list_edited)
    metadata_sql_string = metadata_sql_string[:len(metadata_sql_string)-2]

    # Create database file and open connection
    conn = sqlite3.connect(path_dataset_lastfm2020_db)
    c = conn.cursor()

    # Create separate tables for tags and metadata
    c.execute('CREATE TABLE IF NOT EXISTS tags (' + tags_sql_string + ', PRIMARY KEY(id_dataset))')
    conn.commit()
    c.execute('CREATE TABLE IF NOT EXISTS metadata (' + metadata_sql_string + ', PRIMARY KEY(id_dataset))')
    conn.commit()

    # Insert tags and metadata to the database and close connection
    annotations.to_sql('tags', conn, if_exists = 'replace', index = False)
    metadata.to_sql('metadata', conn, if_exists = 'replace', index= False)
    conn.close()

    print('Done.')

# Function: Pair Spotify ID with tracks in the database and save
def pair_spotify_id():
    # Libraries: Import local
    import pandas as pd
    import sqlite3
    import requests
    from bs4 import BeautifulSoup

    # Libraries: Import custom
    from masters.paths import path_dataset_lastfm2020_db
    from masters.tools_dataset import dataset_delete, folderstruct_lastfm_dataset

    # Check if required folder structure is present
    print('Checking required folder structure.')
    folderstruct_lastfm_dataset()

    # Connect to the database
    conn = sqlite3.connect(path_dataset_lastfm2020_db)
    c = conn.cursor()

    # Select all metadata from the database and make pd.DataFrame
    c.execute('SELECT * FROM metadata WHERE "id_spotify" = "None"')
    columns_names = [description[0] for description in c.description]
    metadata = pd.DataFrame(c.fetchall(), columns=columns_names)

    # Pair Spotify ID for each of tracks using Last.fm wepage
    i = 0
    while i < len(metadata):
        print('\nTrack ' + str(i+1) + '/' + str(len(metadata)))
        print('Pair Spotify ID for track: ' + metadata['artist'][i] + ' - ' + metadata['name'][i])
        url = metadata['url_lastfm'][i]
        id_dataset = metadata['id_dataset'][i]
        # Load track's Last.fm webpage
        try:
            resp = requests.get(url)
            # If load is successful use BeautifulSoup library to get Spotify ID from webpage
            if resp.status_code == 200:
                print('Successfully opened Last.fm page: ' + url)
                soup = BeautifulSoup(resp.text,'html.parser')
                spotify_class = soup.find('a',{'class':'resource-external-link resource-external-link--spotify'})
                # Remove track from the database if there is no Spotify ID for current track
                if spotify_class is None:
                    print('This track does not have Spotify ID associated with Last.fm.')
                    dataset_delete(conn, c, id_dataset)
                    print('Successfully deleted from the database.')
                # Udpate database if Spotify ID is present
                else:
                    spotify_id = spotify_class['href'].split('/')[4]
                    print('Spotify ID found: ' + spotify_id)
                    c.execute('UPDATE metadata SET "id_spotify" = "' + spotify_id + '" WHERE "url_lastfm" = "' + url + '"')
                    conn.commit()
                    print('Successfully saved into the database.')
                i = i + 1
            # Workaround for regional restrictions
            elif resp.status_code == 451:
                print('Unauthorized access. Deleting track...')
                dataset_delete(conn, c, id_dataset)
                print('Successfully deleted from the database.')
                i = i + 1
            else:
                print("Error. Trying to refresh page.")
        except requests.exceptions.TooManyRedirects as e:
            print('Request error: ' + str(e) + '. Deleting track...')
            dataset_delete(conn, c, id_dataset)
            print('Successfully deleted from the database.')
            i = i + 1

    # Close connection to the database file
    conn.close()

    print('Done.')

# Function: Pair Spotify Preview URL with tracks in the databse and save
def pair_spotify_preview_url():
    # Libraries: Import local
    import pandas as pd
    import sqlite3

    # Libraries: Import custom
    from masters.paths import path_dataset_lastfm2020_db
    from masters.tools_dataset import get_spotify, dataset_delete, folderstruct_lastfm_dataset

    # Check if required folder structure is present
    print('Checking required folder structure.')
    folderstruct_lastfm_dataset()

    # Connect to the database
    conn = sqlite3.connect(path_dataset_lastfm2020_db)
    c = conn.cursor()

    # Select all metadata from the database and make pd.DataFrame
    c.execute('SELECT * FROM metadata WHERE "url_spotify_preview" = "None"')
    columns_names = [description[0] for description in c.description]
    metadata = pd.DataFrame(c.fetchall(), columns=columns_names)

    # Use Spotify API to get Spotify Preview URL for each of tracks
    i = 0
    while i < len(metadata):
        id_dataset = metadata['id_dataset'][i]
        id_spotify = metadata['id_spotify'][i]
        print('\nTrack ' + str(i+1) + '/' + str(len(metadata)))
        print ('Pairing Spotify preview URL for track: ' + metadata['artist'][i] + ' - ' + metadata['name'][i])
        # Send Spotify API request and try to get preview URL
        track_spotify_json = get_spotify(id_spotify, api_key_spotify)
        url_spotify_preview = track_spotify_json["preview_url"]
        # Remove track from the database if there is no preview URL
        if url_spotify_preview is None:
            print('This track does not have preview associated with Spotify.')
            dataset_delete(conn, c, id_dataset)
            print('Successfully deleted from the database.')
        else:
            print('Spotify preview URL found: ' + url_spotify_preview)
            c.execute('UPDATE metadata SET "url_spotify_preview" = "' + url_spotify_preview + '" WHERE "id_spotify" = "' + id_spotify + '"')
            conn.commit()
            print('Successfully saved into the database.')
        i = i + 1

    # Close connection to the database file
    conn.close()

    print('Done.')

# Function: Download Spotify Preview
def download_spotify_preview():
    # Libraries: Import local
    import pandas as pd
    import sqlite3
    import requests
    import os

    # Libraries: Import custom
    from masters.paths import path_dataset_lastfm2020_db, path_dataset_lastfm2020
    from masters.tools_dataset import folderstruct_lastfm_dataset

    # Check if required folder structure is present
    print('Checking required folder structure.')
    folderstruct_lastfm_dataset()

    # Connect to the database
    conn = sqlite3.connect(path_dataset_lastfm2020_db)
    c = conn.cursor()

    # Select all metadata from the database and make pd.DataFrame
    c.execute('SELECT * FROM metadata')
    columns_names = [description[0] for description in c.description]
    metadata = pd.DataFrame(c.fetchall(), columns=columns_names)

    # Check if any files are present in the tracks folder
    dir_tracks = os.listdir(path_dataset_lastfm2020 + 'tracks_mp3/')
    dir_tracks = [str(i.split('.', 1)[0]) for i in dir_tracks]

    # Download Spotify Preview (30s MP3) for each of tracks
    i = 0
    while i < len(metadata):
        print('\nTrack ' + str(i+1) + '/' + str(len(metadata)))
        id_dataset = metadata['id_dataset'][i]
        if id_dataset not in dir_tracks:
            print('Downloading Spotify preview for track: ' + metadata['artist'][i] + ' - ' + metadata['name'][i])
            url_spotify_preview = metadata['url_spotify_preview'][i]
            response = requests.get(url_spotify_preview, stream=True)
            if response.status_code == 200:
                open(path_dataset_lastfm2020 + 'tracks_mp3/' + id_dataset + '.mp3', 'wb').write(response.content)
                print('Successfully downloaded.')
                i = i + 1
            else:
                print('Error! Trying to download again.')
        else:
            print('This track is already downloaded: ' + metadata['artist'][i] + ' - ' + metadata['name'][i])
            i = i + 1

    # Close connection to the database file
    conn.close()

    print('Done.')

# Function: Convert tracks to WAV
def convert_to_wav():
    # Libraries: Import local
    import pandas as pd
    import sqlite3
    import os

    # Libraries: Import custom
    from masters.paths import path_dataset_lastfm2020_db, path_dataset_lastfm2020
    from masters.tools_audio import mp3_to_wav
    from masters.tools_dataset import folderstruct_lastfm_dataset, dataset_delete

    # Check if required folder structure is present
    print('Checking required folder structure.')
    folderstruct_lastfm_dataset()

    # Connect to the database
    conn = sqlite3.connect(path_dataset_lastfm2020_db)
    c = conn.cursor()

    # Select all metadata from the database and make pd.DataFrame
    c.execute('SELECT * FROM metadata')
    columns_names = [description[0] for description in c.description]
    metadata = pd.DataFrame(c.fetchall(), columns=columns_names)

    # Check if any files are present in the tracks folder
    dir_tracks = os.listdir(path_dataset_lastfm2020 + 'tracks_wav/')
    dir_tracks = [str(i.split('.', 1)[0]) for i in dir_tracks]

    # Convert each track to WAV
    i = 0
    while i < len(metadata):
        print('\nTrack ' + str(i+1) + '/' + str(len(metadata)))
        id_dataset = metadata['id_dataset'][i]
        if id_dataset not in dir_tracks:
            print('Converting preview for track: ' + metadata['artist'][i] + ' - ' + metadata['name'][i])
            path_track_mp3 = path_dataset_lastfm2020 + 'tracks_mp3/' + id_dataset + '.mp3'
            path_track_wav = path_dataset_lastfm2020 + 'tracks_wav/' + id_dataset + '.wav'
            mp3_to_wav(path_track_mp3, path_track_wav)
            print('Successfully converted.')
            i = i + 1
        else:
            print('Track is already converted: ' + metadata['artist'][i] + ' - ' + metadata['name'][i])
            i = i + 1

    # Check if converting was successful
    dir_tracks = os.listdir(path_dataset_lastfm2020 + 'tracks_wav/')
    dir_tracks = [str(i.split('.', 1)[0]) for i in dir_tracks]
    for i in range(len(metadata['id_dataset'])):
        print('\nTrack ' + str(i+1) + '/' + str(len(metadata)))
        print ('Verifying preview for track: ' + metadata['artist'][i] + ' - ' + metadata['name'][i])
        id_dataset = str(metadata['id_dataset'][i])
        if not id_dataset in dir_tracks:
            dataset_delete(conn, c, id_dataset)
            print('Track preview is missing. Successfully deleted from the database.')
        else:
            print('Successfully verified.')

    # Close connection to the database file
    conn.close()

    print('Done.')

# Function: Compute mel spectrogram for each track and save
def compute_melgram():
    # Libraries: Import local
    import numpy as np
    import pandas as pd
    import sqlite3
    import os

    # Libraries: Import custom
    from masters.paths import path_dataset_lastfm2020_db, path_dataset_lastfm2020
    from masters.tools_audio import melgram
    from masters.tools_dataset import folderstruct_lastfm_dataset, dataset_delete

    # Check if required folder structure is present
    print('Checking required folder structure.')
    folderstruct_lastfm_dataset()

    # Connect to the database
    conn = sqlite3.connect(path_dataset_lastfm2020_db)
    c = conn.cursor()

    # Select all metadata from the database and make pd.DataFrame
    c.execute('SELECT * FROM metadata')
    columns_names = [description[0] for description in c.description]
    metadata = pd.DataFrame(c.fetchall(), columns=columns_names)

    # Check if any files are present in the melgram folder
    dir_features = os.listdir(path_dataset_lastfm2020 + 'features_melgram/')
    dir_features = [str(i.split('.', 1)[0]) for i in dir_features]

    # Compute mel spectrogram for each of tracks
    i = 0
    while i < len(metadata):
        print('\nTrack ' + str(i+1) + '/' + str(len(metadata)))
        print ('Computing melgram for track: ' + metadata['artist'][i] + ' - ' + metadata['name'][i])
        id_dataset = metadata['id_dataset'][i]
        if id_dataset not in dir_features:
            path_track_wav = path_dataset_lastfm2020 + 'tracks_wav/' + id_dataset + '.wav'
            melgram_computed = melgram(path_track_wav)
            # Remove extra data if track is longer than expected
            if np.size(melgram_computed, axis=1) >= 1366:
                melgram_computed = np.delete(melgram_computed, range(1366,np.size(melgram_computed, axis=1)), 1)
                np.save(path_dataset_lastfm2020 + 'features_melgram/' + id_dataset + '.npy', melgram_computed)
                print('Successfully computed.')
            # Remove track from the database if track is shorter than expected
            elif np.size(melgram_computed, axis=1) < 1366:
                dataset_delete(conn, c, id_dataset)
                print('The track is not 1366 samples long. Successfully deleted from the database.')
            i = i + 1
        else:
            print('Melgram is already computed: ' + metadata['artist'][i] + ' - ' + metadata['name'][i])
            i = i + 1

    # Close connection to the database file
    conn.close()

    print('Done.')

# Parser for command line switches and parameters
parser = argparse.ArgumentParser(description='This script builds Last.fm 2020 Dataset. You can edit variables "n_tags" and "n_tracks" in this script to get different number of tags and tracks per tag. Script runs all steps (1-6) if no switch or parameter is provided. !!! WARNING !!!: You need to insert valid Last.fm API and Spotify API key to this script!!!')
parser.add_argument('--build_db', action='store_true', help='Step 1: Use Last.fm API to get most popular tags and tracks associated with these tags and save everything to the SQLite database file.')
parser.add_argument('--pair_spotify_id', action='store_true', help='Step 2: Pair Spotify IDs with tracks. (Parsing Last.fm webpage for each track.)')
parser.add_argument('--pair_spotify_preview_url', action='store_true', help='Step 3: Pair Spotify Preview URLs with tracks. (Using Spotify API)')
parser.add_argument('--download_spotify_preview', action='store_true', help='Step 4: Download Spotify previews.')
parser.add_argument('--convert_to_wav', action='store_true', help='Step 5: Convert Spotify previews to WAV.')
parser.add_argument('--compute_melgram', action='store_true', help='Step 6: Compute and save mel spectrogram for each of tracks.')
args = parser.parse_args()

# Run step by step if no switches or parameters are provided
if args.build_db == False and args.pair_spotify_id == False and args.pair_spotify_preview_url == False and args.download_spotify_preview == False and args.convert_to_wav == False and args.compute_melgram == False:
    build_db(n_tags, n_tracks)
    pair_spotify_id()
    pair_spotify_preview_url()
    download_spotify_preview()
    convert_to_wav()
    compute_melgram()

# Definition of what each switch or parameter does
for arg_current in vars(args):
    if arg_current == 'build_db' and args.build_db == True:
        build_db(n_tags, n_tracks)
    elif arg_current == 'pair_spotify_id' and args.pair_spotify_id == True:
        pair_spotify_id()
    elif arg_current == 'pair_spotify_preview_url' and args.pair_spotify_preview_url == True:
        pair_spotify_preview_url()
    elif arg_current == 'download_spotify_preview' and args.download_spotify_preview == True:
        download_spotify_preview()
    elif arg_current == 'convert_to_wav' and args.convert_to_wav == True:
        convert_to_wav()
    elif arg_current == 'compute_melgram' and args.compute_melgram == True:
        compute_melgram()
