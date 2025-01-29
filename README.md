# S2 Java SDK

> [!WARNING]
> This SDK is actively in development, and not officially supported yet!

A Java SDK for interacting with the S2 streaming service.

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
./gradlew publishToMavenLocal
```

### Using Maven (Local Published Artifact)

Add this dependency to your `pom.xml`:

```xml

<dependency>
  <groupId>dev.s2</groupId>
  <artifactId>s2-sdk</artifactId>
  <version>0.0.5-SNAPSHOT</version>
</dependency>
```

### Using Gradle (Kotlin DSL)

Add this dependency to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("dev.s2:s2-sdk:0.0.5-SNAPSHOT")
}
```

## Project Structure

- `s2/` - The main SDK module.
- `s2-internal/` - Code and types generated from
  the [S2 protobuf definitions](https://github.com/s2-streamstore/s2-protos).
- `app/` - Example application demonstrating SDK usage.

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

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.
