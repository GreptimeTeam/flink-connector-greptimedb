# GreptimeDB Apache Flink Connector

`flink-connector-greptimedb` is a reusable Apache Flink sink connector for writing application records into GreptimeDB.

## Maven Dependency

```xml
<dependency>
    <groupId>io.greptime</groupId>
    <artifactId>flink-connector-greptimedb</artifactId>
    <version>${version}</version>
</dependency>
```

## Usage

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

#### Delivery Guarantee

The SQL/Table sink currently provides at-least-once delivery. Failed writes, job retries, or recovery can produce duplicate writes, so design GreptimeDB tables and downstream queries to accept that semantic.
The connector does not provide an exactly-once commit protocol.

#### Unsupported Features

The SQL/Table connector currently does not support:

* SQL/Table source
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

To test against a different GreptimeDB image:

```bash
mvn -Pintegration-test verify -Dgreptimedb.test.image=greptime/greptimedb:latest
```

## Compatibility Notes

* Java 17
* Apache Flink 2.0.x
* GreptimeDB Java ingester 0.15.0
