/*
 * This file was generated by the Gradle 'init' task.
 *
 * This project uses @Incubating APIs which are subject to change.
 */

plugins {
    application
    java
}

repositories {
    google()
    mavenCentral()
    mavenLocal()
}

val grpcVersion = "1.64.0"

dependencies {
    implementation(project(":s2-sdk"))
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("javax.annotation:javax.annotation-api:1.3.2")
    implementation("ch.qos.logback:logback-classic:1.2.6")
    implementation("org.slf4j:slf4j-api:1.7.32")
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
}

val executables = listOf(
    "org.example.app.AccountDemo",
    "org.example.app.BasinDemo",
    "org.example.app.ManagedReadSessionDemo",
    "org.example.app.ManagedAppendSessionDemo",
)

executables.forEach { mainClassName ->
    val name = mainClassName.substringAfterLast('.')
    tasks.register<JavaExec>("run$name") {
        group = "application"
        description = "Run the $name demo app."
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set(mainClassName)
    }
}


tasks.test {
    useJUnitPlatform()
}

java {
    withSourcesJar()
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(19))
    }
}
