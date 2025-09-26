<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-16.4_LTS-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/aws/en/release-notes/runtime/16.4lts.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?style=for-the-badge)](https://databricks.com/try-databricks)
[![BPIPE](https://img.shields.io/badge/BPIPE-3.25.3--1-orange?style=for-the-badge)](https://data.bloomberg.com/)

## B-Pipe spark reader

Bloomberg B-Pipe (Bloomberg Data License - Real-Time Feed) is an enterprise-grade data feed that provides real-time
market data, such as live quotes, trades, order book updates, and reference information, directly into firms’ internal
systems. Unlike Bloomberg Terminal (for humans), B-Pipe powers automated trading platforms, risk engines, portfolio
systems, and analytics applications. It is designed for low-latency, high-reliability, institutional use.
Data from B-Pipe often must be published into a firm’s internal message bus (e.g. Kafka) to decouple raw Bloomberg
sessions from consuming applications, adding operational overhead, increasing latency and requiring complex mapping and
transformation. We introduce a lightweight, open-source connector that allows Databricks customers to directly stream
B-Pipe data into Spark (and Spark Streaming) pipelines without relying on intermediate enterprise buses or heavy
middleware, simplifying architecture and enabling real-time analytics whilst ensuring built-in support for bloomberg
entitlements and audit trails through unity catalog.

<br>

> *We are building an open-source B-Pipe to Spark connector to radically simplify market data ingestion, unlocking
real-time financial analytics on Databricks without the traditional complexity of enterprise middleware.*
>
> Antoine Amend, Financial Services GTM Leader

## Reference Architecture

Although B-Pipe market data can be used for a variety of use cases, the same can be directly leveraged for risk
management, particularly for Value at Risk (VaR) calculations, by providing the high-frequency, high-quality price
inputs necessary to model market exposure accurately.
We represent below how this library can be used to simplify risk management practices on the Databricks Intelligence
Platform.

![reference_architecture.png](images%2Freference_architecture.png)

## Usage

### Market data

`//blp/mktdata` is the Bloomberg API service that streams real-time market data (quotes, trades, market depth) to client
applications by subscription. Live data from the exchanges, it is critical to ensure delivery to specific applications
only by tracking entitlement
and lineage through unity catalog. A B-Pipe feed of market data must be limited to a given application only.

|                |                     |
|----------------|---------------------|
| B-Pipe service | `//blp/mktdata`     |
| Delivery mode  | publish / subscribe |
| Spark mode     | streaming           |

```python
market_stream = (

    spark
      # mktData is a streaming endpoint
      .readStream
      .format("//blp/mktData")

      # B-PIPE connection
      .option("serverAddresses", "['SERVER1', 'SERVER2']")
      .option("serverPort", 8194)
      .option("tlsCertificatePath", "rootCertificate.pk7")  # on classpath
      .option("tlsPrivateKeyPath", "privateKey.pk12")  # on classpath
      .option("tlsPrivateKeyPassword", "password")
      .option("authApplicationName", "app+name")

      # Service configuration
      .option("fields", "['MKTDATA_EVENT_TYPE','MKTDATA_EVENT_SUBTYPE','EID','BID','ASK','IS_DELAYED_STREAM','TRADE_UPDATE_STAMP_RT']")
      .option("securities", "['BBHBEAT Index', 'GBP BGN Curncy', 'EUR BGN Curncy', 'JPYEUR BGN Curncy']")

      # Custom logic
      .option("timezone", "UTC")
      .option("permissive", value = true)
      
      .load()
)
```

### Reference data

|                |                       |
|----------------|-----------------------|
| B-Pipe service | `//blp/staticMktData` |
| Delivery mode  | request / response    |
| Spark mode     | batch                 |

#### ReferenceDataRequest

`ReferenceDataRequest` is a type of API call (using Bloomberg’s BLPAPI) that asks for metadata or static attributes
about securities — things that generally do not change tick-by-tick, such as Security descriptions, ISINs, CUSIPs,
SEDOLs, Exchange codes, Sector classifications, etc. We mapped its service again spark application accepting below
parameters.

```python
reference_df = (
    
    spark
      # staticMktData is a batch endpoint
      .read
      .format("//blp/staticMktData")

      # B-PIPE connection
      .option("serverAddresses", "['SERVER1', 'SERVER2']")
      .option("serverPort", 8194)
      .option("tlsCertificatePath", "rootCertificate.pk7") # on classpath
      .option("tlsPrivateKeyPath", "privateKey.pk12")  # on classpath
      .option("tlsPrivateKeyPassword", "password")
      .option("authApplicationName", "app+name")
      .option("correlationId", 999)

      # Service configuration
      .option("serviceName", "ReferenceDataRequest")
      .option("fields", "['EID', 'BID', 'ASK', 'LAST_PRICE']")
      .option("securities", "['BBHBEAT Index', 'GBP BGN Curncy', 'EUR BGN Curncy', 'JPYEUR BGN Curncy']")
      .option("returnEids", true)

      # Start batch ingest
      .load()
)
```

## Partitioning

Given the distributed nature of spark, one can specify the number of partitions we want to distribute each request
against. Each partition will be responsible for a specific B-Pipe request against a subset of securities or a given time
window.
We support 3 modes of partitioning.

- **Partition By Date**: for B-Pipe requests having a `startDate` and `endDate` parameters, we can easily split one
  B-Pipe request into multiple requests where each partition will be responsible for a specific non overlapping time
  window.
- **Partition By Security**: for B-Pipe requests with multiple `securities` provided as argument, we can easily
  distribute multiple securities to multiple requests. Suboptimal in specific cases where portfolio is made of
  securities of different liquidity (different traded volumes), this might remove the bottleneck of streaming an entire
  portfolio through 1 single request.
- **Smart Partitioning** (advanced): for B-Pipe requests with multiple `securities` provided as argument, we can
  explicitly tell framework which securities will be bundled together across multiple partitions, ensuring better
  latency and resilience in a distributed environment.

See notebook [03_bpipe_partitioning.ipynb](databricks%2F03_bpipe_partitioning.ipynb) for more information about
partitioning. This partitioning logic can be visualized as follows

![images](images/smart_partitioning.png)

## Install

Download B-Pipe library available on DATA<GO> [website](https://data.bloomberg.com/) and run the following maven
command. This will build a jar file that can be installed on a databricks environment as external library (
see [cluster libraries](https://docs.databricks.com/aws/en/libraries/cluster-libraries)). Make sure to include a
directory that include certificate and private keys to be made available on executors' classpath.

```shell
mvn clean install \
  -Dbloomberg.jar.path=/path/to/blpapi-3.19.1-1.jar \
  -Dbloomberg.cert.path=/path/to/bpipe-certificates
```

## References

- https://data.bloomberglp.com/professional/sites/10/2017/03/BLPAPI-Core-Developer-Guide.pdf
- https://www.bloomberg.com/professional/blog/webinar/demystifying-the-market-data-feed/
- https://github.com/Robinson664/bemu

## Project support

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks
with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do
not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is
provided subject to the Databricks [License](./LICENSE.md). All included or referenced third party libraries are subject
to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be
reviewed as time permits, but there are no formal SLAs for support.

## Authors

<antoine.amend@databricks.com>

## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this project is provided subject to the Databricks
License [https://databricks.com/db-license-source].
Code does not contain external libaries besides those included in the Databricks supported runtime.

| library      | description                     | license    | source                             |
|--------------|---------------------------------|------------|------------------------------------|
| Scala        | high-level programming language | Apache 2.0 | https://www.scala-lang.org/license |
| Apache Spark | distributed computing framework | Apache 2.0 | https://spark.apache.org/          |
