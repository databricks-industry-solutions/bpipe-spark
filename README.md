<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

## B-PIPE spark reader
TODO: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

## Reference Architecture
IMAGE TO REFERENCE ARCHITECTURE

## Authors
<antoine.amend@databricks.com>

## Usage

### Market data

TODO: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

|                |  |
|----------------| ----------- |
| B-pipe service | `//blp/mktdata` |
| Delivery mode      | publish / subscribe |
| Spark mode      | streaming |

```python
spark \
  .readStream \
  .format("//blp/mktdata") \
  .option("serviceHost", "127.0.0.1") \
  .option("servicePort", 8954) \
  .option("correlationId", 999) \
  .option("fields", "['BID','ASK','TRADE_UPDATE_STAMP_RT']") \
  .option("securities", "['SPY US EQUITY','MSFT US EQUITY']") \
  .load()
```

### Reference data

TODO: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

|                |  |
|----------------| ----------- |
| B-pipe service | `//blp/refdata` |
| Delivery mode      | request / response |
| Spark mode      | batch |

#### ReferenceDataRequest

TODO: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

```python
spark \
  .read \
  .format("//blp/refdata") \
  .option("serviceName", "ReferenceDataRequest") \
  .option("serviceHost", "127.0.0.1") \
  .option("servicePort", 8954) \
  .option("correlationId", 999) \
  .option("fields", "['PX_LAST','BID','ASK','TICKER','CHAIN_TICKERS']") \
  .option("securities", "['SPY US EQUITY','MSFT US EQUITY','AAPL 150117C00600000 EQUITY']")
  .option("overrides", "{'CHAIN_PUT_CALL_TYPE_OVRD':'C','CHAIN_POINTS_OVRD':'4','CHAIN_EXP_DT_OVRD':'20141220'}") \
  .load()
```

#### IntradayTickRequest

TODO: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

```python
spark \
  .read \
  .format("//blp/refdata") \
  .option("serviceName", "IntradayTickRequest") \
  .option("serviceHost", "127.0.0.1") \
  .option("servicePort", 8954) \
  .option("correlationId", 999) \
  .option("partitions", 5) \
  .option("security", "SPY US EQUITY") \
  .option("startDateTime", "2022-11-01") \
  .load()
```

#### IntradayBarRequest

TODO: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

```python
spark \
  .read \
  .format("//blp/refdata") \
  .option("serviceName", "IntradayBarRequest") \
  .option("serviceHost", "127.0.0.1") \
  .option("servicePort", 8954) \
  .option("correlationId", 999) \
  .option("interval", 60) \
  .option("partitions", 5) \
  .option("security", "SPY US EQUITY") \
  .option("startDateTime", "2022-11-01") \
  .load()
```

## Install

To work, one would need to download bpipe library available on DATA<GO> [website](https://data.bloomberg.com/) and run the following maven command.
This will build a jar file that can be installed on a databricks environment (see [cluster libraries](https://docs.databricks.com/aws/en/libraries/cluster-libraries)).

```shell
mvn clean install -Dbloomberg.jar.path=/path/to/blpapi-3.19.1-1.jar
```

For testing purpose, one can download JAR of bpipe emulator available on [github](https://github.com/Robinson664/bemu) that mimics bpipe basic functionalities with synthetic data.

## References

- https://data.bloomberglp.com/professional/sites/10/2017/03/BLPAPI-Core-Developer-Guide.pdf
- https://github.com/Robinson664/bemu
- https://www.bloomberg.com/professional/blog/webinar/demystifying-the-market-data-feed/


## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE.md). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 

## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
