# GreptimeDB Apache Flink Connector

`flink-connector-greptimedb` provides Apache Flink connectors for writing records to GreptimeDB and reading bounded tables through Flink SQL.

## Maven Dependency

```xml
<dependency>
    <groupId>io.greptime</groupId>
    <artifactId>flink-connector-greptimedb</artifactId>
    <version>${version}</version>
</dependency>
```

## Usage

### SQL/Table Source Usage

The SQL/Table source performs a bounded, single-task scan through the GreptimeDB MySQL protocol.

```sql
CREATE TEMPORARY TABLE cpu_metrics_source (
    ts TIMESTAMP(3),
    host STRING,
    usage DOUBLE,
    observed_on DATE
) WITH (
    'connector' = 'greptimedb',
    'query.jdbc-url' = 'jdbc:mysql://127.0.0.1:4002/public?useSSL=false',
    'database' = 'public',
    'table' = 'cpu_metrics',
    'query.fetch-size' = '1000'
);

SELECT ts, host, usage, observed_on FROM cpu_metrics_source;
```

The connector artifact does not include MySQL Connector/J. Add the driver separately to every Flink SQL Client or cluster runtime that executes the source. For example:

```bash
./bin/sql-client.sh embedded \
    -j /path/to/flink-connector-greptimedb-${version}-shaded.jar \
    -j /path/to/mysql-connector-j-8.4.0.jar
```

#### SQL/Table Source Options

| Option | Required | Description |
| --- | --- | --- |
| `connector` | Yes | Must be `greptimedb`. |
| `query.jdbc-url` | Yes | GreptimeDB MySQL JDBC URL. It must not contain credentials, authentication tokens, password-bearing properties, `connectTimeout`, or `socketTimeout`. |
| `database` | No | GreptimeDB database. Defaults to `public`. |
| `table` | No | GreptimeDB table name. Defaults to the Flink table identifier. |
| `username` | No | GreptimeDB username. Configure together with `password`. |
| `password` | No | GreptimeDB password. Configure together with `username`. |
| `query.connect-timeout-ms` | No | JDBC connection timeout in milliseconds. Defaults to `10000`. |
| `query.socket-timeout-ms` | No | JDBC socket timeout in milliseconds. Defaults to `300000`. |
| `query.fetch-size` | No | JDBC statement fetch-size hint. Must be non-negative and defaults to `0`. |

Supported source column types are `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `CHAR`, `VARCHAR`/`STRING`, `BINARY`, `VARBINARY`, `DATE`, `DECIMAL`, and `TIMESTAMP` without a time zone.

`query.fetch-size` is passed to the JDBC statement only when it is positive. Connector/J cursor fetching also depends on driver settings such as `useCursorFetch=true` in the JDBC URL; this connector does not modify the URL automatically.

The source does not currently support projection, filter, or limit pushdown, lookup reads, streaming reads, CDC, `TIMESTAMP_LTZ`, metadata discovery, or schema preflight. It always produces insert-only rows and does not create GreptimeDB tables.

### DataStream Sink Usage

```java
import io.greptime.flink.sink.GreptimeSink;
import io.greptime.models.DataType;
import io.greptime.models.TableSchema;

TableSchema tableSchema = TableSchema.newBuilder("cpu_metrics")
        .addTimestamp("ts", DataType.TimestampMillisecond)
        .addTag("host", DataType.String)
        .addField("usage", DataType.Float64)
        .build();

GreptimeSink<CpuMetric> sink = GreptimeSink.<CpuMetric>builder()
        .endpoint("127.0.0.1:4001")
        .database("public")
        .tableSchema(tableSchema)
        .batchSize(1_000)
        .recordSerializer(metric -> new Object[] {
                metric.timestampMillis(),
                metric.host(),
                metric.usage()
        })
        .build();

stream.sinkTo(sink);
```

If GreptimeDB authentication is enabled, configure it through the builder:

```java
GreptimeSink<CpuMetric> sink = GreptimeSink.<CpuMetric>builder()
        .endpoint("127.0.0.1:4001")
        .database("public")
        .tableSchema(tableSchema)
        .plainTextAuth("username", "password")
        .recordSerializer(metric -> new Object[] {
                metric.timestampMillis(),
                metric.host(),
                metric.usage()
        })
        .build();
```

### SQL/Table Sink Usage

The connector also provides an insert-only SQL/Table sink for writing Flink table rows into GreptimeDB.
The time-index column must be a non-null `TIMESTAMP` or `TIMESTAMP_LTZ` column. Flink timestamp columns are nullable by default, so declare the time-index column with `NOT NULL`.

```sql
CREATE TEMPORARY TABLE cpu_metrics (
    ts TIMESTAMP(3) NOT NULL,
    host STRING,
    usage DOUBLE
) WITH (
    'connector' = 'greptimedb',
    'endpoints' = '127.0.0.1:4001',
    'database' = 'public',
    'table' = 'cpu_metrics',
    'time-index' = 'ts',
    'tags' = 'host',
    'batch.max-rows' = '1000'
);

INSERT INTO cpu_metrics VALUES
    (TIMESTAMP '2024-01-02 03:04:05.000', 'host-a', 0.42);
```

#### SQL/Table Sink Options

| Option | Required | Description |
| --- | --- | --- |
| `connector` | Yes | Must be `greptimedb`. |
| `endpoints` | Yes | Comma-separated GreptimeDB ingester endpoints. |
| `time-index` | Yes | Name of the non-null timestamp column used as the GreptimeDB time index. |
| `database` | No | GreptimeDB database. Defaults to `public`. |
| `table` | No | GreptimeDB table name. Defaults to the Flink table identifier. |
| `username` | No | GreptimeDB username. Configure together with `password`. |
| `password` | No | GreptimeDB password. Configure together with `username`. |
| `tags` | No | Comma-separated column names to write as GreptimeDB tag columns. |
| `batch.max-rows` | No | Maximum rows per write batch. Defaults to `1000`. |
| `bulk.timeout-ms-per-message` | No | Timeout in milliseconds for each bulk write message. Defaults to `60000`. |
| `bulk.max-requests-in-flight` | No | Maximum in-flight requests for the bulk stream. Defaults to `8`. |
| `bulk.allocator-init-reservation-bytes` | No | Initial Arrow allocator reservation in bytes. Defaults to `0`. |
| `bulk.allocator-max-allocation-bytes` | No | Maximum Arrow allocator allocation in bytes. Defaults to `1073741824`. |

#### Delivery Guarantee

The SQL/Table sink currently provides at-least-once delivery. Failed writes, job retries, or recovery can produce duplicate writes, so design GreptimeDB tables and downstream queries to accept that semantic.
The connector does not provide an exactly-once commit protocol.

#### Unsupported Features

The SQL/Table connector currently does not support:

* Lookup source
* CDC or streaming source
* Update, delete, or retract changelog rows
* Primary-key upsert semantics
* Automatic table creation or auto-DDL
* Exactly-once commit protocol

## Local Build

```bash
mvn test
mvn package
```

`mvn package` produces both the regular connector jar and an attached shaded jar:

* `target/flink-connector-greptimedb-${version}.jar` is the thin jar for Maven-based applications.
* `target/flink-connector-greptimedb-${version}-shaded.jar` is the deployment jar for Flink SQL Client or cluster classpath usage. It bundles and relocates the connector runtime dependencies while keeping Flink dependencies provided by the Flink runtime.

Use the shaded jar when loading the connector directly in Flink SQL Client:

```bash
./bin/sql-client.sh embedded \
    -j /path/to/flink-connector-greptimedb-${version}-shaded.jar
```

## Integration Test

Integration tests are isolated behind the `integration-test` Maven profile and require Docker.
The profile configures the JVM options required by Apache Arrow on Java 17.

```bash
mvn -Pintegration-test verify
```

To run only the GreptimeDB sink integration test:

```bash
mvn -Pintegration-test -Dit.test=GreptimeSinkIT verify
```

To run only the GreptimeDB SQL/Table sink integration test:

```bash
mvn -Pintegration-test -Dit.test=GreptimeDynamicTableSinkIT verify
```

To run only the GreptimeDB SQL/Table source integration test:

```bash
mvn -Pintegration-test -Dit.test=GreptimeDynamicTableSourceIT verify
```

To test against a different GreptimeDB image:

```bash
mvn -Pintegration-test verify -Dgreptimedb.test.image=greptime/greptimedb:latest
```

## Compatibility Notes

* Java 17
* Apache Flink 2.0.x
* GreptimeDB Java ingester 0.15.0
* MySQL Connector/J 8.4.x for SQL/Table source reads
