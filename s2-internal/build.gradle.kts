plugins {
    `java-library`
    id("com.google.protobuf") version "0.9.4"
    id("maven-publish")
}

group = "dev.s2"
version = project.rootProject.property("version") as String

repositories {
    mavenCentral()
}

val grpcVersion = "1.64.0"
val protobufVersion = "3.25.0"
val tomcatAnnotationsVersion = "11.0.2"
val javaxAnnotationVersion = "1.3.2"
val mockitoVersion = "5.8.0"
val assertJVersion = "3.24.2"

dependencies {
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("javax.annotation:javax.annotation-api:$javaxAnnotationVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("org.slf4j:slf4j-api:1.7.32")
    compileOnly("org.apache.tomcat:tomcat-annotations-api:$tomcatAnnotationsVersion")
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation(libs.junit.jupiter)
    testImplementation("io.grpc:grpc-testing:$grpcVersion")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
    testImplementation("org.mockito:mockito-core:$mockitoVersion")
    testImplementation("org.mockito:mockito-junit-jupiter:$mockitoVersion") {
        exclude(group = "org.junit.jupiter")
    }
    testImplementation("uk.org.webcompere:system-stubs-jupiter:2.1.7")
    testImplementation("org.assertj:assertj-core:$assertJVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                create("grpc")
            }
        }
    }
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}


publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifactId = "s2-internal"
            pom {
                name.set("Generated code for S2 SDK.")
                description.set("Generated code and types used by the S2 SDK.")
                url.set("https://github.com/s2-streamstore/s2-sdk-java") // Replace with your repository URL

                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
            }
        }
    }

}
