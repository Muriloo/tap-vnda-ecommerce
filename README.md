# tap-vnda-ecommerce

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from [VNDA Ecommerce Platform's API](https://demo.vnda.com.br/api/v2/docs/index.html)
- Extracts the following resources:
  - [Orders](https://demo.vnda.com.br/api/v2/docs/index.html#operation/get-api-v2-orders)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state 
- Do a full replication based on a start_date

## Context
VNDA is an Ecommerce platform system used by small and medium companies in Brazil.

## Configuration

This tap requires a config.json which specifies details about API URL, API Token and Start Date. An example of the config.json is as below:
```
{
  'api_url': 'https://yourdomain.com.br/api',
  'api_token': '<your token>',
  'start_date': '2020-12-01T00:00:00.000000Z'
}
```

## Streams

[**orders**](https://ajuda.bling.com.br/hc/pt-br/articles/360046424094-GET-pedidos)
- Endpoint: https://yourdomain.com.br/api/v2/orders/
- Primary Key: id
- Replication strategy:
    - Incremental, based on 'updated_at' column
    - Do a request for each page until empty page is found
- Transformation:
    - None relevant


---

Copyright &copy; 2020 Stitch
