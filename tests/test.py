from src.beedis.datadis import Datadis, ENDPOINTS
import json
from datetime import datetime
import pandas as pd

f = open("config.json", "r")
config = json.load(f)

Datadis.connection(config['datadis']['username'], config['datadis']['password'], timezone="UTC", timeout=100)
data = Datadis.datadis_query(ENDPOINTS.GET_PUBLIC, start_date=datetime(2022, 8, 1), end_date=datetime(2022, 10, 31),
                             page=0,
                             community="09", postal_code="25001")

df = pd.DataFrame(data)

supplies = Datadis.datadis_query(ENDPOINTS.GET_SUPPLIES)
contract = Datadis.datadis_query(ENDPOINTS.GET_CONTRACT, cups=supplies[0]['cups'],
                                 distributor_code=supplies[0]['distributorCode'])
consumption = Datadis.datadis_query(ENDPOINTS.GET_CONSUMPTION, cups=supplies[0]['cups'],
                                    distributor_code=supplies[0]['distributorCode'],
                                    start_date=datetime(2023, 1, 28), end_date=datetime(2023, 1, 28),
                                    measurement_type="0",
                                    point_type=supplies[0]['pointType'])

max_power = Datadis.datadis_query(ENDPOINTS.GET_MAX_POWER, cups=supplies[0]['cups'],
                                  distributor_code=supplies[0]['distributorCode'], start_date=datetime(2023, 2, 1),
                                  end_date=datetime(2023, 2, 1))
