plugins {
    id("buildlogic.java-library-conventions")
    id("com.google.protobuf") version "0.9.4"
    id("maven-publish")
}

repositories {
    mavenCentral()
}

val grpcVersion = "1.64.0"
val protobufVersion = "3.25.0"
val tomcatAnnotationsVersion = "11.0.2"
val javaxAnnotationVersion = "1.3.2"

dependencies {
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    
    implementation("javax.annotation:javax.annotation-api:$javaxAnnotationVersion")

    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    
    compileOnly("org.apache.tomcat:tomcat-annotations-api:$tomcatAnnotationsVersion")


    
    testImplementation(libs.junit.jupiter)
    testImplementation("io.grpc:grpc-testing:$grpcVersion")
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

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:deprecation")
}

tasks.test {
    useJUnitPlatform()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

publishing {
  publications {
    create<MavenPublication>("maven") {
      groupId = "org.twelvehart"
      artifactId = "s2"
      version = "0.0.1"
      from(components["java"])
    }
  }
}