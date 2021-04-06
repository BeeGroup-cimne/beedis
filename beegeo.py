# Read functions file and configuration
import progressbar as progressbar
from dateutil.relativedelta import relativedelta
import pandas as pd
from datadis import datadis, ENDPOINTS
import os
import datetime as dt
import time
import json


def create_sub_wd(wd):
    """
    Define the different sub working directories to store and gather the data
    :param wd:
    :return:
    """

    wd_q = "%s/data/datadis" % wd

    for w in [wd_q]:
        os.makedirs(w, exist_ok=True)

    return wd_q


def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month


def datadis_query(wd, config):
    # Initialize connection to Datadis
    datadis.connection(config['datadis']['username'], config['datadis']['password'])

    for province_code in config["provinces"]:

        date_ini_i = dt.datetime.strptime(config["datadis"]["date_ini"], "%Y-%m-%d").date()

        date_end = dt.datetime.strptime(config["datadis"]["date_end"], "%Y-%m-%d").date()

        print('Downloading DATADIS consumption data from province {province_code}.')
        bar = progressbar.ProgressBar(maxval=diff_month(date_end, date_ini_i) + 1)
        os.makedirs("%s/%s" % (wd, province_code), exist_ok=True)

        i = 0
        bar.update(i)

        while date_ini_i < date_end:
            if str(date_ini_i)[0:7] not in os.listdir(
                    "%s/%s" % (wd, province_code)) or "consumption.csv" not in os.listdir(
                    "%s/%s/%s" % (wd, province_code, str(date_ini_i)[0:7])):
                os.makedirs("%s/%s/%s" % (wd, province_code, str(date_ini_i)[0:7]), exist_ok=True)

                # TODO: DATADIS CHANGED ITS FUNCTION AND REQUESTING WITH PROVINCE CODE DOES NOT RETURN POSTAL CODE DATA.
                # TODO: This is returning the aggregated by province only
                datadis_dataset = datadis.datadis_query(ENDPOINTS.GET_PUBLIC, start_date=date_ini_i,
                                                        end_date=(date_ini_i + relativedelta(months=1) - relativedelta(
                                                            days=1)), page=0,
                                                        community="09", province_municipality=province_code)

                pd.DataFrame(datadis_dataset).to_csv(
                    "%s/%s/%s/consumption.csv" % (wd, province_code, str(date_ini_i)[0:7]),
                    sep="\t")

            date_ini_i += relativedelta(months=+1)

            i += 1
            time.sleep(0.1)
            bar.update(i)


with open('config.json') as config_file:
    configs = json.load(config_file)

# Generate by default all the subdirectories
wd_p = create_sub_wd(wd=configs['wd'])

datadis_query(wd=wd_p, config=configs)
