# Load needed libraries
import copy, re, sys, urllib, zipfile, os, time, requests, forecastio, pytz, json, subprocess, progressbar, shutil
from pymongo import MongoClient
import datetime as dt


def create_sub_wd(wd):
    """
    Define the different sub working directories to store and gather the data
    :param wd:
    :return:
    """
    
    wd_q = "%s/data/datadis" % wd


    for w in [wd_q]:
        os.makedirs(w, exist_ok=True)

    return(wd_q)


def datadis_session(headers,config):

    s = requests.Session()
    s.headers = headers
    r = s.get("https://datadis.es/nikola-auth/login", verify=False)
    r = s.post("https://datadis.es/nikola-auth/login",
               data={"username": config['datadis']['username'], "password": config['datadis']['password']},
               verify=False)
    r = s.post('https://datadis.es/nikola-auth/tokens/login', headers=headers,
               params=(('username', config['datadis']['username']), ('password', config['datadis']['password'])),
               verify=False)

    return(s)


def datadis_headers():

    return(
        {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36',
                'accept': 'text/plain',
                'Host': 'datadis.es',
                'Connection': 'keep-alive',
                'Content-Length': '0',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
                'accept': 'text/plain',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36',
                'Origin': 'https://datadis.es',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
                'Referer': 'https://datadis.es/api-dataset',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9'
        }
    )


def datadis_params(date_ini, date_end, page, province_code):

    return(
        {
            'startDate': date_ini,
            'endDate': date_end,
            'page': page,
            'pageSize': '200',
            'measurePointType': '',
            'community': '',
            'distributor': '',
            'fare': '',
            'postalCode': '',
            'provinceMunicipality': province_code,
            'economicSector': '',
            'tension': '',
            'timeDiscrimination': '',
            'sort': ''
        }
    )


def diff_month(d1, d2):

    return (d1.year - d2.year) * 12 + d1.month - d2.month


def datadis_query(wd, config):

    # Initialize connection to Datadis
    s = datadis_session(datadis_headers(),config)

    for province_code in config["provinces"]:

        date_ini_i = dt.datetime.strptime(config["datadis"]["date_ini"], "%Y-%m-%d").date()

        date_end = dt.datetime.strptime(config["datadis"]["date_end"],"%Y-%m-%d").date()

        print(f'\nDownloading DATADIS consumption data from province {province_code}.')
        bar = progressbar.ProgressBar(max_value=diff_month(date_end,date_ini_i)+1)
        os.makedirs("%s/%s" % (wd, province_code), exist_ok=True)

        i = 0
        bar.update(i)

        while date_ini_i < date_end:

            datadis_dataset = []

            if not str(date_ini_i)[0:7] in os.listdir("%s/%s" % (wd, province_code)) or \
               not "consumption.csv" in os.listdir("%s/%s/%s" % (wd, province_code,str(date_ini_i)[0:7])):

                os.makedirs("%s/%s/%s" % (wd, province_code, str(date_ini_i)[0:7]), exist_ok=True)

                more_data = True
                page = 0

                while more_data:

                    s.headers['Content-Type'] = 'application/json'
                    print(page)
                    params = datadis_params(date_ini = date_ini_i.strftime("%Y/%m/%d"),
                                            date_end = (date_ini_i + relativedelta(months=1) - relativedelta(days=1)).strftime("%Y/%m/%d"),
                                            page = str(page),
                                            province_code = province_code)

                    response = s.get('https://datadis.es/api-public/api-search', params=params, verify=False)

                    if response.status_code!=200 or response.text[0:24]=='\n\n\n\n\n\n\n\n\n<!DOCTYPE html>':

                        while 'errorPass' in s.cookies.get_dict() or \
                              response.status_code!=200 or \
                              response.text[0:24]=='\n\n\n\n\n\n\n\n\n<!DOCTYPE html>':
                            del s
                            time.sleep(2)
                            s = datadis_session(datadis_headers(), config)
                            s.headers['Content-Type'] = 'application/json'
                            response = s.get('https://datadis.es/api-public/api-search', params=params, verify=False)

                    if len(response.json()) != 0:
                        datadis_dataset.extend(json.loads(response.text.encode("latin_1").decode("utf_8")))
                        page += 1
                    else:
                        more_data = False

                pd.DataFrame(datadis_dataset).to_csv("%s/%s/%s/consumption.csv" % (wd, province_code, str(date_ini_i)[0:7]),
                                                    sep="\t")

            date_ini_i += relativedelta(months=+1)

            i += 1
            time.sleep(0.1)
            bar.update(i)
