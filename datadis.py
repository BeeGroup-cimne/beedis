import time
from enum import Enum
import requests
import json
from datetime import datetime, timedelta
import pandas as pd

timezone_source = "Europe/Madrid"


def __dummy_parse__(response: dict) -> dict:
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


def __consumption_parse__(result: dict) -> dict:
    timezone = datadis.timezone
    df = pd.DataFrame(result)
    df['date'] = pd.to_datetime(df.date)
    df['time'] = pd.to_timedelta(df.time + ":00") - timedelta(hours=1)
    df['datetime'] = pd.to_datetime(df.date+df.time)
    df = df.set_index('datetime')
    df = df.tz_localize(timezone_source, ambiguous="infer").tz_convert(timezone)
    df = df.sort_index()
    df = df.drop(["date", "time"], 1)
    df = df.reset_index()
    data = df.to_dict(orient="records")
    for i in data:
        i.update({"datetime": i['datetime'].to_pydatetime()})
    return data


def __public_params__(start_date: datetime, end_date: datetime, page: int, community: str, page_size: int = 200,
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
        parse = __dummy_parse__

    class GET_PUBLIC(Enum):
        url = 'https://datadis.es/api-public/api-search'
        params = __public_params__
        parse = __dummy_parse__


class datadis(object):
    session = None
    username = None
    password = None
    timezone = None

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
    def connection(cls, username: str, password: str, timezone="UTC"):
        cls.username = username
        cls.password = password
        cls.session.headers = datadis.__datadis_headers__()
        cls.timezone = timezone
        cls.__login__()

    @classmethod
    def __login__(cls):
        cls.session = requests.Session()
        cls.session.get("https://datadis.es/nikola-auth/login")
        cls.session.post("https://datadis.es/nikola-auth/login",
                         data={"username": username, "password": password})
        cls.session.post('https://datadis.es/nikola-auth/tokens/login', headers=cls.session.headers,
                         params=(('username', username), ('password', password)))

    @classmethod
    def __datadis_request__(cls, url, params):
        response = cls.session.get(url, params=params)
        if response.status_code != 200 or response.text[0:24] == '\n\n\n\n\n\n\n\n\n<!DOCTYPE html>':
            retries = 0
            while 'errorPass' in cls.session.cookies.get_dict() or \
                    response.status_code != 200 or \
                    response.text[0:24] == '\n\n\n\n\n\n\n\n\n<!DOCTYPE html>':
                retries += 1
                if retries >= 20:
                    raise Exception("Failed with too many retries")
                del cls.session
                time.sleep(2)
                cls.__login__()
                response = cls.session.get(url, params=params)
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
