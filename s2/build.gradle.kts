plugins {
    `java-library`
    id("com.google.protobuf") version "0.9.4"
    id("maven-publish")
    id("net.researchgate.release") version "3.1.0"
}

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
    
    compileOnly("org.apache.tomcat:tomcat-annotations-api:$tomcatAnnotationsVersion")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation(libs.junit.jupiter)
    testImplementation("io.grpc:grpc-testing:$grpcVersion")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
    testImplementation("org.mockito:mockito-core:$mockitoVersion")
    testImplementation("org.mockito:mockito-junit-jupiter:$mockitoVersion") {
        exclude(group = "org.junit.jupiter")
    }
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

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:deprecation")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
      events("passed", "skipped", "failed")
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
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/ASRagab/s2-java-sdk")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
  }
}

release {
    git {
        requireBranch.set("main")
        pushToRemote.set("origin")
        signTag.set(false)
        tagTemplate.set("v\${version}")
    }
}

tasks.afterReleaseBuild {
    dependsOn(tasks.publish)
}