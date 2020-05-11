"""
========================= !!! READ ME !!! =========================
This script contains definitons of functions for work with OS.
Make sure you have installed all requirements from requirements.txt
===================================================================
"""

# Libraries: Import global
import os
import datetime
import string
import random

# Function: Make current date string
def currdate():
    now = datetime.datetime.now()
    return now.strftime('%y%m%d-%H%M%S')

# Function: Make folder in specific path
def makedir(path):
    try:
        os.mkdir(path)
    except OSError as err:
        # print('Creation of the directory ' + path + ' failed')
        print('OS error: {0}'.format(err))
    else:
        print('Successfully created the directory ' + path)

# Function Make random string
def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))