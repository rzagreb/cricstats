import os

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
APP_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
PROJECT_DIR = os.path.dirname(APP_DIR)

DATA_DIR = os.path.join(PROJECT_DIR, "data")

DATA_RAW_DIR = os.path.join(DATA_DIR, "01_raw")
DATA_UNZIPPED_DIR = os.path.join(DATA_DIR, "02_unzipped")
DATA_DB_DIR = os.path.join(DATA_DIR, "03_db")
