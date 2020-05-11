"""
========================= !!! READ ME !!! =========================
This script contains definitions of functions for audio processing 
and analysis.
Make sure you have installed all requirements from requirements.txt
===================================================================
"""

# Libraries: Import global
import numpy as np
import librosa
import os
import subprocess

# Function: Compute mel spectrogram
def melgram(filename):
    x, fs = librosa.load(filename, sr=12000)
    result = librosa.feature.melspectrogram(y=x, sr=fs, hop_length=256, n_fft=512, n_mels=96) # power mel spectrogram
    result_dB = librosa.power_to_db(result, ref=np.max) # convert power mel spectrogram to dB and normalize to max value
    return result_dB

# Function: Convert audio file to WAV
def mp3_to_wav(filename_input, filename_output):
    path_ff = os.path.abspath('./masters/3rd_party/ffmpeg.exe') # 'os.system' needs an absolute path for calling ffmpeg.exe
    filename_input = '"' + os.path.relpath(filename_input) + '"'
    filename_output = '"' + os.path.relpath(filename_output) + '"'
    subprocess.run('"' + path_ff + '" -y -i ' + filename_input + ' -acodec pcm_s16le -ac 1 -ar 12000 -loglevel error ' + filename_output)