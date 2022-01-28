from src.beedis.datadis import datadis, ENDPOINTS
import json
from datetime import datetime
import pandas as pd

f = open("config.json", "r")
config = json.load(f)

datadis.connection(config['datadis']['username'], config['datadis']['password'], timezone="UTC", timeout=10)
data = datadis.datadis_query(ENDPOINTS.GET_PUBLIC, start_date=datetime(2020, 10, 1), end_date=datetime(2020, 10, 31),
                             page=0,
                             community="09", postal_code="25001")

df = pd.DataFrame(data)

supplies = datadis.datadis_query(ENDPOINTS.GET_SUPPLIES)
contract = datadis.datadis_query(ENDPOINTS.GET_CONTRACT, cups=supplies[0]['cups'],
                                 distributor_code=supplies[0]['distributorCode'])
consumption = datadis.datadis_query(ENDPOINTS.GET_CONSUMPTION, cups=supplies[0]['cups'],
                                    distributor_code=supplies[0]['distributorCode'],
                                    start_date=datetime(2021, 4, 29), end_date=datetime.now(),
                                    measurement_type="0",
                                    point_type=supplies[0]['pointType'])

max_power = datadis.datadis_query(ENDPOINTS.GET_MAX_POWER, cups=supplies[0]['cups'],
                                  distributor_code=supplies[0]['distributorCode'], start_date=datetime(2021, 8, 1),
                                  end_date=datetime(2021, 12, 1))
