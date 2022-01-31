import time
from enum import Enum
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
from bs4 import BeautifulSoup


timezone_source = "Europe/Madrid"


def __dummy_parse__(response: list) -> list:
    return response


def __contract_params__(cups: str, distributor_code: str, authorized_nif: str = ''):
    return {
        "cups": cups,
        "distributorCode": distributor_code,
        "authorizedNif": authorized_nif
    }


def __supplies_params__(authorized_nif: str = ''):
    return {
        "authorizedNif": authorized_nif
    }


def __get_max_power__(cups: str, distributor_code: str, start_date: datetime, end_date: datetime,
                      authorized_nif: str = ''):
    return {
        "cups": cups,
        "distributorCode": distributor_code,
        "startDate": start_date.strftime("%Y/%m"),
        "endDate": end_date.strftime("%Y/%m"),
        "authorizedNif": authorized_nif
    }


def __consumption_params__(cups: str, distributor_code: str, start_date: datetime, end_date: datetime,
                           measurement_type: str,
                           point_type: str, authorized_nif: str = ''):
    return {
        "cups": cups,
        "distributorCode": distributor_code,
        "startDate": start_date.strftime("%Y/%m/%d"),
        "endDate": end_date.strftime("%Y/%m/%d"),
        "measurementType": measurement_type,
        "pointType": point_type,
        "authorizedNif": authorized_nif
    }


def __max_power_parse__(result: list) -> list:
    timezone = datadis.timezone
    df = pd.DataFrame(result)
    if not df.empty:
        df_final = pd.DataFrame()
        df['month'] = pd.to_datetime(df.date.apply(lambda x: "/".join(x.split("/")[:-1]+["01"])))
        df['date'] = pd.to_datetime(df.date)
        df['time'] = pd.to_timedelta(df.time + ":00") - timedelta(hours=1)
        for month, df_tmp in df.groupby("month"):
            try:
                df_tmp['datetime'] = pd.to_datetime(df_tmp.date+df_tmp.time)
                df_tmp = df_tmp.set_index('datetime')
                df_tmp = df_tmp.tz_localize(timezone_source, ambiguous="infer").tz_convert(timezone)
                df_tmp = df_tmp.drop(["date", "time"], 1)
                df_tmp = df_tmp.reset_index()
                df_tmp = df_tmp.pivot(index="month", columns="period", values=["datetime", "maxPower"])
                df_tmp.columns = [f"{c}_period_{p}" for c, p in df_tmp.columns]
                df_final = df_final.append(df_tmp)
            except Exception as e:
                print(f"There was an error in the {day}: {e}")
        df_final.sort_index(inplace=True)
        df_final.reset_index(inplace=True)
        df_final.rename({"month": "datetime"}, axis=1, inplace=True)
        data = df_final.to_dict(orient="records")
        for i in data:
            for f in [x for x in i.keys() if x.startswith("datetime")]:
                i[f] = i[f].to_pydatetime()
        return data
    else:
        return list()


def __consumption_parse__(result: list) -> list:
    timezone = datadis.timezone
    df = pd.DataFrame(result)
    if not df.empty:
        df_final = pd.DataFrame()
        df['date'] = pd.to_datetime(df.date)
        df['time'] = pd.to_timedelta(df.time + ":00") - timedelta(hours=1)
        for day, df_tmp in df.groupby("date"):
            try:
                df_tmp['datetime'] = pd.to_datetime(df_tmp.date+df_tmp.time)
                df_tmp = df_tmp.set_index('datetime')
                df_tmp = df_tmp.tz_localize(timezone_source, ambiguous="infer").tz_convert(timezone)
                df_tmp = df_tmp.sort_index()
                df_tmp = df_tmp.drop(["date", "time"], 1)
                df_tmp = df_tmp.reset_index()
                df_final = df_final.append(df_tmp)
            except Exception as e:
                print(f"There was an error in the {day}: {e}")
        df_final.set_index('datetime', inplace=True)
        df_final.sort_index(inplace=True)
        df_final.reset_index(inplace=True)
        data = df_final.to_dict(orient="records")
        for i in data:
            i.update({"datetime": i['datetime'].to_pydatetime()})
        return data
    else:
        return list()


def __public_params__(start_date: datetime, end_date: datetime, page: int, community: str, page_size: int = 2000,
                      measure_point_type: str = '', province_municipality: str = '', distributor: str = '',
                      fare: str = '', postal_code: str = '', economic_sector: str = '', tension: str = '',
                      time_discrimination: str = '',
                      sort: str = ''):
    return {
        'startDate': start_date.strftime("%Y/%m/%d"),
        'endDate': end_date.strftime("%Y/%m/%d"),
        'page': page,
        'pageSize': page_size,
        'measurePointType': measure_point_type,
        'community': community,
        'distributor': distributor,
        'fare': fare,
        'postalCode': postal_code,
        'provinceMunicipality': province_municipality,
        'economicSector': economic_sector,
        'tension': tension,
        'timeDiscrimination': time_discrimination,
        'sort': sort
    }


class ENDPOINTS(Enum):
    class GET_SUPPLIES(Enum):
        url = 'https://datadis.es/api-private/api/get-supplies'
        params = __supplies_params__
        parse = __dummy_parse__

    class GET_CONTRACT(Enum):
        url = 'https://datadis.es/api-private/api/get-contract-detail'
        params = __contract_params__
        parse = __dummy_parse__

    class GET_CONSUMPTION(Enum):
        url = 'https://datadis.es/api-private/api/get-consumption-data'
        params = __consumption_params__
        parse = __consumption_parse__

    class GET_MAX_POWER(Enum):
        url = 'https://datadis.es/api-private/api/get-max-power'
        params = __get_max_power__
        parse = __max_power_parse__

    class GET_PUBLIC(Enum):
        url = 'https://datadis.es/api-public/api-search'
        params = __public_params__
        parse = __dummy_parse__


class datadis(object):
    session = None
    username = None
    password = None
    timezone = None
    timeout = None
    @staticmethod
    def __datadis_headers__() -> dict:
        return {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (HTML, like Gecko)'
                          ' Chrome/85.0.4183.121 Safari/537.36',
            'accept': 'text/plain',
            'Host': 'datadis.es',
            'Connection': 'keep-alive',
            'Content-Length': '0',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Origin': 'https://datadis.es',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://datadis.es/api-dataset',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',

        }

    # 'Content-Type': 'application/json'
    @classmethod
    def connection(cls, username: str, password: str, timezone="UTC", timeout=None):
        cls.username = username
        cls.password = password
        cls.timeout = timeout
        cls.timezone = timezone
        cls.__login__()

    @classmethod
    def __login__(cls):
        cls.session = requests.Session()
        cls.session.headers = datadis.__datadis_headers__()
        cls.session.get("https://datadis.es/nikola-auth/login", timeout=cls.timeout)
        first_login = cls.session.post("https://datadis.es/nikola-auth/login",
                                       data={"username": cls.username, "password": cls.password})

        soup = BeautifulSoup(first_login.text, 'html.parser')
        error = soup.find("label", class_='error')
        if error:
            raise PermissionError(error.text.strip())

        second_login = cls.session.post('https://datadis.es/nikola-auth/tokens/login', headers=cls.session.headers,
                                        params=(('username', cls.username), ('password', cls.password)))
        if not second_login.ok:
            raise PermissionError("Invalid user_password for second login")

    @classmethod
    def __datadis_request__(cls, url, params, timeout=None):
        response = cls.session.get(url, params=params, timeout=cls.timeout)
        if response.status_code != 200 or response.text[0:24] == '\n\n\n\n\n\n\n\n\n<!DOCTYPE html>':
            del cls.session
            time.sleep(1)
            cls.__login__()
            response = cls.session.get(url, params=params, timeout=cls.timeout)
            if response.status_code != 200:
                raise ConnectionError(f"The response returned : {response.status_code}")
            elif response.text[0:24] == '\n\n\n\n\n\n\n\n\n<!DOCTYPE html>':
                raise ConnectionError(f"The response returned : Error Message")
        return response

    @classmethod
    def datadis_query(cls, endpoint, **kwargs):
        endpoint_enum = endpoint.value
        url = endpoint_enum.url.value
        params = endpoint_enum.params(**kwargs)
        parse = endpoint_enum.parse
        if 'page' in params:
            datadis_dataset = []
            more_data = True
            page = params['page']
            while more_data:
                print(page)
                params['page'] = page
                response = datadis.__datadis_request__(url, params=params)
                if len(response.json()) != 0:
                    datadis_dataset.extend(json.loads(response.text.encode("latin_1").decode("utf_8")))
                    page += 1
                else:
                    more_data = False
            data = datadis_dataset
        else:
            response = datadis.__datadis_request__(url, params=params)
            data = json.loads(response.text.encode("latin_1").decode("utf_8"))
        return parse(data)
