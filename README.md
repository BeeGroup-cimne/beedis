# beedis
This library is used to obtain the data available from datadis at the current version on 6-04-2021

#### Usage

- import the library
```
from beedis import datadis, ENDPOINTS
```

- connect using dadadis credentials

```
datadis.connection('username', 'password', timezone="UTC")
```

- use the different endpoints available in the datadis api

```
datadis.datadis_query(endpoint, **kwargs)
```

#### ENDPOINTS
params with * are required

GET_SUPPLIES:
> Returns the supplies of the user or the ones of the authorized nif

    params: 
        - authorizedNif : str

GET_CONTRACT
> Returns the contracts of the user supplies or the ones of the authorized nif supplies

    params:
        - cups: str *
        - distributorCode: str *
        - authorized_nif: str


GET_CONSUMPTION
> Returns the consumption of the supply, 
>
> the data will be based on opening times: EX: consumption indicated at "02:00" will belong to the consumption between 02:00 and 03:00
> 
> measurement type must be "0" for hourly data or "1" for quarty hourly data

       params:
        - cups: str *
        - distributorCode: str *
        - start_date: datetime *
        - end_date: datetime *
        - measurement_type: str *
        - pointType: str *
        - authorizedNif: str



GET_MAX_POWER
> Returns the max power of the supply, 
 

    params:
        - cups: str *
        - distributorCode: str *
        - start_date: datetime *
        - end_date: datetime *
        - authorizedNif: str

GET_PUBLIC
> Returns the public data aggregated by different fields
    
    params: 
        - start_date: datetime *
        - end_date: datetime *
        - page: int *
        - community: str *,
        - pageSize: int,
        - measurePointType: str
        - distributor: str
        - fare: str
        - postalCode: str
        - provinceMunicipality: str
        - economicSector: str
        - tension: str,
        - timeDiscrimination: str
        - sort: strs