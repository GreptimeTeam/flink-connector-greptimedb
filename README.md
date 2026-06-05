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

To test against a different GreptimeDB image:

```bash
mvn -Pintegration-test verify -Dgreptimedb.test.image=greptime/greptimedb:latest
```

## Compatibility Notes

* Java 17
* Apache Flink 2.0.x
* GreptimeDB Java ingester 0.15.0
