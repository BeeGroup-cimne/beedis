# Read functions file and configuration
from utils.functions import *
with open('config.json') as config_file:
    config = json.load(config_file)

# Generate by default all the subdirectories
wd_q = create_sub_wd(wd=config['wd'])

# Download, preprocessing and upload to MongoDB the Datadis aggregated consumption datasets and its related meteo data
datadis_query(wd=wd_q, config=config)
