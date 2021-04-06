# beedis
This library is used to obtain the data available from datadis at the current version on 6-04-2021

#### Usage

- import the library
```
from datadis import datadis, ENDPOINTS
```

- connect using dadadis credentials

```
datadis.connection('username', 'password')
```

- use the different endpoints available in the datadis api

```
datadis.datadis_query(endpoint, **kwargs)
```

#### ENDPOINTS
params with * are required

GET_SUPPLIES:
    
    params: 
        - authorizedNif : str

GET_CONTRACT

    params:
        - cups: str *
        - distributorCode: str *
        - authorized_nif: str


GET_CONSUMPTION

    params:
        - cups: str *
        - distributorCode: str *
        - start_date: datetime *
        - end_date: datetime *
        - measurement_type: str *
        - pointType: str *
        - authorizedNif: str

GET_MAX_POWER

    params:
        - cups: str *
        - distributorCode: str *
        - start_date: datetime *
        - end_date: datetime *
        - authorizedNif: str

GET_PUBLIC
    
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