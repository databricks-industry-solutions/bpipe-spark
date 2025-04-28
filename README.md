<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

## B-Pipe spark reader

Bloomberg B-Pipe (Bloomberg Data License - Real-Time Feed) is an enterprise-grade data feed that provides real-time market data, such as live quotes, trades, order book updates, and reference information, directly into firms’ internal systems. Unlike Bloomberg Terminal (for humans), B-Pipe powers automated trading platforms, risk engines, portfolio systems, and analytics applications. It is designed for low-latency, high-reliability, institutional use.
Data from B-Pipe often must be published into a firm’s internal message bus (e.g. Kafka) to decouple raw Bloomberg sessions from consuming applications, adding operational overhead, increasing latency and requiring complex mapping and transformation.
We introduce a lightweight, open-source connector that allows Databricks customers to directly stream B-Pipe data into Spark (and Spark Streaming) pipelines without relying on intermediate enterprise buses or heavy middleware, simplifying architecture and enabling real-time analytics whilst ensuring built-in support for bloomberg entitlements and audit trails through unity catalog.

> We are building an open-source B-Pipe to Spark connector to radically simplify market data ingestion, unlocking real-time financial analytics on Databricks — without the traditional complexity of enterprise middleware. Antoine Amend, Financial Services GTM Leader.

## Authors
<antoine.amend@databricks.com>

## Usage

### Market data

|                |  |
|----------------| ----------- |
| B-Pipe service | `//blp/mktdata` |
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

|                |  |
|----------------| ----------- |
| B-Pipe service | `//blp/refdata` |
| Delivery mode      | request / response |
| Spark mode      | batch |

#### HistoricalDataRequest

```python
spark \
  .read \
  .format("//blp/refdata") \
  .option("serviceName", "HistoricalDataRequest") \
  .option("serviceHost", "127.0.0.1") \
  .option("servicePort", 8954) \
  .option("correlationId", 999) \
  .option("fields", "['BID', 'ASK']") \
  .option("securities", "['SPY US EQUITY','MSFT US EQUITY','AAPL 150117C00600000 EQUITY']") \
  .option("startDate", "2022-01-01") \
  .load()
```

#### ReferenceDataRequest

```python
spark \
  .read \
  .format("//blp/refdata") \
  .option("serviceName", "ReferenceDataRequest") \
  .option("serviceHost", "127.0.0.1") \
  .option("servicePort", 8954) \
  .option("correlationId", 999) \
  .option("fields", "['PX_LAST','BID','ASK','TICKER','CHAIN_TICKERS']") \
  .option("securities", "['SPY US EQUITY','MSFT US EQUITY','AAPL 150117C00600000 EQUITY']") \
  .option("overrides", "{'CHAIN_PUT_CALL_TYPE_OVRD':'C','CHAIN_POINTS_OVRD':'4','CHAIN_EXP_DT_OVRD':'20141220'}") \
  .load()
```

#### IntradayTickRequest

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

To work, one would need to download B-Pipe library available on DATA<GO> [website](https://data.bloomberg.com/) and run the following maven command.
This will build a jar file that can be installed on a databricks environment (see [cluster libraries](https://docs.databricks.com/aws/en/libraries/cluster-libraries)).

```shell
mvn clean install -Dbloomberg.jar.path=/path/to/blpapi-3.19.1-1.jar
```

For testing purpose, one can download JAR of B-Pipe emulator available on [github](https://github.com/Robinson664/bemu) that mimics B-Pipe basic functionalities with synthetic data.

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
