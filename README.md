# S2 Java SDK

> [!WARNING]
> This SDK is actively in development, and not officially supported yet!

A Java SDK for interacting with the S2 streaming service. This SDK provides a hopefully convenientinterface for working with S2's gRPC-based streaming API.

## Features

- Fluent builder pattern for client configuration
- Synchronous and asynchronous stream operations
- Basin and stream management
- Automatic channel management for basin switching

## Prerequisites

- Java 17 or higher
- Gradle 8.5 or higher
- An S2 account and bearer token

### Building from Source

1. Clone the repository:
```bash
git clone --recurse-submodules https://github.com/s2-streamstore/s2-sdk-java 
cd s2-sdk-java
```

2. Build the project:
```bash
./gradlew build
```

3. Install to local Maven repository:
```bash
./gradlew s2:publishToMavenLocal
```

### Using Maven (Local Published Artifact)

Add this dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>dev.s2</groupId>
    <artifactId>s2</artifactId>
    <version>0.0.1</version>
</dependency>
```

### Using Gradle (Kotlin DSL)

Add this dependency to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("dev.s2:s2:0.0.1")
}
```

## Quick Start

Here's a simple example that demonstrates the basic usage of the SDK:

```java
import s2.services.Client;
import s2.v1alpha.*;

// Create a client
var client = Client.newBuilder()
    .host("aws.s2.dev")
    .port(443)
    .bearerToken("your-token")
    .build();

// List all basins
var basins = client.account().listBasins("");
basins.forEach(basin -> 
    System.out.printf("Basin: %s (state: %s)%n", 
        basin.getName(), 
        basin.getState())
);

// Create a new basin
var basinConfig = BasinConfig.newBuilder()
    .setDefaultStreamConfig(
        StreamConfig.newBuilder()
            .setStorageClass(StorageClass.STORAGE_CLASS_STANDARD)
            .build()
    )
    .build();

var newBasin = client.account().createBasin("my-basin", basinConfig);

// Switch to the basin
client.useBasin("my-basin");

// Create a stream
var streamConfig = StreamConfig.newBuilder()
    .setStorageClass(StorageClass.STORAGE_CLASS_STANDARD)
    .build();

client.basin().createStream("my-stream", streamConfig);

// Append a record
var record = AppendRecord.newBuilder()
    .setBody(ByteString.copyFromUtf8("Hello, S2!"))
    .build();

var appendOutput = client.stream().append("my-stream", List.of(record));

// Read records
var readOutput = client.stream().read("my-stream", 0, null);
if (readOutput.hasBatch()) {
    readOutput.getBatch().getRecordsList().forEach(r -> 
        System.out.printf("Record %d: %s%n", 
            r.getSeqNum(), 
            r.getBody().toStringUtf8())
    );
}
```

## Project Structure

- `s2/` - The main SDK module
  - `src/main/java/s2/services/` - Core service implementations
  - `src/main/java/s2/channel/` - Channel management
  - `src/main/java/s2/auth/` - Authentication handling
  - `src/main/proto/` - Protocol Buffer definitions
- `app/` - Example application demonstrating SDK usage

## Running the Example App

1. Set required environment variables:
```bash
export S2_HOST=aws.s2.dev
export S2_PORT=443
export S2_TOKEN=your-token
```

2. Run the example:
```bash
./gradlew app:run
```

## Advanced Usage

### Asynchronous Operations

The SDK supports asynchronous operations through the `AsyncStreamService`:

```java
// Async append
client.streamAsync().appendAsync(streamName, records)
    .thenAccept(output -> 
        System.out.printf("Append completed: %d to %d%n", 
            output.getStartSeqNum(), 
            output.getEndSeqNum())
    );

// Streaming read session
client.streamAsync().openReadSession(
    streamName,
    0,
    null,
    response -> System.out.println("Received records: " + response.getBatch().getRecordsCount()),
    error -> System.err.println("Error: " + error.getMessage())
);
```

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.
