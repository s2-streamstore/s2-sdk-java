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

dependencies {
    compileOnly(libs.tomcat.annotations)
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.stub)
    implementation(libs.javax.annotation.api)
    implementation(libs.protobuf.java)
    implementation(libs.slf4j.api)
    testImplementation(libs.assertj.core)
    testImplementation(libs.grpc.inprocess)
    testImplementation(libs.grpc.testing)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter) {
        exclude(group = "org.junit.jupiter")
    }
    testImplementation(libs.system.stubs.jupiter)
    testImplementation(platform(libs.junit.bom))
    testRuntimeOnly(libs.junit.platform.launcher)
}

val protobufVersion: String = libs.versions.protobuf.get()
val grpcVersion: String = libs.versions.grpc.get()
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
        languageVersion.set(JavaLanguageVersion.of(11))
    }
    withJavadocJar()
    withSourcesJar()
}


publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifactId = "s2-internal"
            pom {
                name.set("Generated code for S2 SDK.")
                description.set("Generated code and types used by the S2 SDK.")
                url.set("https://github.com/s2-streamstore/s2-sdk-java")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                scm {
                    connection = "scm:git:git@github.com:s2-streamstore/s2-sdk-java.git"
                    url = "https://github.com/s2-streamstore/s2-sdk-java"
                }
            }
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/s2-streamstore/s2-sdk-java")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }

}
