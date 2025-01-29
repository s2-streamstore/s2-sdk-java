plugins {
    id("net.researchgate.release") version "3.1.0"
    id("maven-publish")
}

release {
    // Disable the default checks since we're using GitHub Actions
    failOnUnversionedFiles = false
    failOnUpdateNeeded = false
    failOnCommitNeeded = false
    git {
        requireBranch.set("") // Since we use GHA to release this should be okay
        pushToRemote.set("origin")
        signTag.set(false)
        tagTemplate.set("v\${version}")
    }
}


tasks.named("afterReleaseBuild") {
    dependsOn("publish")
}
