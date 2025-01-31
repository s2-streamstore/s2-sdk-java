# S2 Java SDK

> [!WARNING]
> This SDK is actively in development, and not officially supported yet!

A Java SDK for interacting with the S2 streaming service.

#### Current Java API Documentation

- [s2-sdk @ 0.0.12](https://s2-streamstore.github.io/s2-sdk-java/javadocs/s2-sdk/0.0.12/)
- [s2-internal @ 0.0.12](https://s2-streamstore.github.io/s2-sdk-java/javadocs/s2-internal/0.0.12/)

## Prerequisites

- Java 17 or higher
- Gradle 8.5 or higher
- An S2 account and bearer token

### Building from Source

1. Clone the repository:

```bash
git clone \
  --recurse-submodules \
  https://github.com/s2-streamstore/s2-sdk-java 

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

### Using Maven (with GitHub Packages)

Packages listed on this repo can be downloaded from GitHub's Maven repository. This document goes
through [the details](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry),
but typically this involves:

- Creating a new personal access token with `read:packages` permissions
- Configuring your
  `~/.m2/settings.xml` to look like the
  example [here](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry#authenticating-with-a-personal-access-token)
    - The relevant URL would be `https://maven.pkg.github.com/s2-streamstore/s2-sdk-java`
- Adding the relevant package(s) to your project's `pom.xml` (each
  individual [package](https://github.com/s2-streamstore/s2-sdk-java/packages/) should list the XML
  representation of the dependency)

### Using Maven (Local Published Artifact)

Add this dependency to your `pom.xml`:

```xml

<dependency>
  <groupId>dev.s2</groupId>
  <artifactId>s2-sdk</artifactId>
  <version><!--Use the current version specified in `gradle.properties`--></version>
</dependency>
```

### Using Gradle (Kotlin DSL)

Add this dependency to your `build.gradle.kts`:

```kotlin
// pick a version from the releases, or specified in `gradle.properties`
var s2SdkVersion = "SOMETHING"
dependencies {
    implementation("dev.s2:s2-sdk:$s2SdkVersion")
}
```

## Project Structure

- `s2-sdk/` - The main SDK module.
- `s2-internal/` - Code and types generated from
  the [S2 protobuf definitions](https://github.com/s2-streamstore/s2-protos).
- `app/` - Example application demonstrating SDK usage.

## Running the Example Apps

The example apps contain some simple demo uses of the SDK.

For all of these, you will need an S2 account. Sign up on [s2.dev](https://s2.dev/) if you haven't
already, and generate an auth token [in the dashboard](https://s2.dev/dashboard).

From there, you can use the [S2 CLI](https://github.com/s2-streamstore/s2-cli) for creating new
basins and streams (or, try doing this using the SDK!).

For the demos discussed below, it will be helpful to create a new basin and stream.

Start by setting some environment variables in your shell.

```bash
export S2_AUTH_TOKEN="MY-SECRET"
export S2_BASIN="my-demo-java"
export S2_STREAM="test/1"
```

Then, if you need to create the basin or stream, you can do so with the CLI:

```bash
s2 create-basin "s2://${S2_BASIN}"
s2 create-stream "s2://${S2_BASIN}/${S2_STREAM}"
```

### Appending and reading records

Assuming you've defined those variables above, you can start a managed append session that will try
to append 50k random records (a total of ~500MiB) to your stream:

```bash
./gradlew runManagedAppendSessionDemo
```

Similarly, you can use a managed read session to read those records:

```bash
./gradlew runManagedReadSessionDemo
```

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.
