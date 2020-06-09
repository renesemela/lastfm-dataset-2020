# lastfm-dataset-2020 (!!! UNDER CONSTRUCTION !!!)
New Last.fm Dataset 2020 for music auto-tagging purposes. This dataset is based on the concept of the original [Last.fm Dataset](http://millionsongdataset.com/lastfm/) which is based on the [Million Song Dataset](http://millionsongdataset.com/). This dataset is a by-product of my masters's thesis (see [repository](https://github.com/renesemela/masters-thesis-music-autotagging)).

There are **122877 tracks** and **100 tags** in the dataset. For each track, you can find metadata (Last.fm track URL, **Spotify MP3 Preview URL for download**, ...).

## Getting Started
These instructions will help you to get familiar with the concept of this dataset.

### Prerequisites
You will need any tool which is capable to work with SQlite databases (eg. *sqlite3* for *Python*).

### Database file
The main file of the dataset is located at ***datasets\lastfm_dataset_2020\lastfm_dataset_2020.db***. Feel free to use this file in any way you want. As said above -> it is SQlite database and I am sure that the internal structure of the DB file will be piece of cake for you. There are two tables in the database file - ***metadata*** (contains metadata such as *id_dataset*, *url_lastfm*, *url_spotify_preview*) and ***tags*** (one-hot coding of 100 tags for each track).

### Dataset ready Python script
In the root folder, you can find Python script ***dataset_lastfm.py*** which I created for building this dataset during my thesis. You can use this script to download Spotifiy Preview MP3 files. You can also use this script to convert MP3 previews to WAV. And in the end, you can use this script to compute Mel Spectrogram for each track as proposed in the most of the auto-tagging related works.

You just need to use this script in this way:
```
python dataset_lastfm.py --help
python dataset_lastfm.py --download_spotify_preview
python dataset_lastfm.py --convert_to_wav
python dataset_lastfm.py --compute_melgram
```

You can get more info about this script in my master's thesis [repository](https://github.com/renesemela/masters-thesis-music-autotagging).

## Built With
* Python
* sqlite3
* [Librosa](https://librosa.github.io/)
* [SciPy](https://scipy.org/)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments
* [BDALab](https://bdalab.utko.feec.vutbr.cz/) - Hardware resources

## Note
This readme is a bit lightweight so feel free to ask me anything. Thanks for using this dataset! <3
