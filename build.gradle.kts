plugins {
    id("java")
    id("application")
    id("com.github.johnrengelman.shadow") version "8.1.1"  // uber-jar 생성을 위한 플러그인
    //
}

group = "com.musinsa"
version = "1.0-SNAPSHOT"

// Flink 버전 정의
val flinkVersion = "1.17.2"

repositories {
    mavenCentral()
}

dependencies {
    // Flink Core 의존성
    implementation("org.apache.flink:flink-streaming-java:${flinkVersion}")
    implementation("org.apache.flink:flink-clients:${flinkVersion}")

    // Flink Table API & SQL
    implementation("org.apache.flink:flink-table-api-java-bridge:${flinkVersion}")
    implementation("org.apache.flink:flink-table-planner-loader:${flinkVersion}")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("org.slf4j:slf4j-log4j12:2.0.9")

    // Test
    testImplementation("org.apache.flink:flink-test-utils:${flinkVersion}")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}

tasks {
    // Gradle 테스트 설정
    test {
        useJUnitPlatform()
    }

    // Shadow JAR 설정
    shadowJar {
        manifest {
            attributes["Main-Class"] = "com.musinsa.WordCountBatchProcess" // 메인 클래스 경로로 수정하세요
        }
        // 중복된 의존성 처리
        mergeServiceFiles()
        exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    }
}

// 기본 JAR 태스크 대신 shadowJar를 사용하도록 설정
tasks.jar {
    enabled = false
}
tasks.build {
    dependsOn(tasks.shadowJar)
}
