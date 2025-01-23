package com.musinsa;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // Flink 실행 환경 생성
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 간단한 데이터 스트림 생성
        DataStream<String> dataStream = env.fromElements("Hello", "Flink", "World");

        // 데이터 스트림 처리
        dataStream
                .map(String::toUpperCase)  // 대문자로 변환
                .filter(word -> word.startsWith("F")) // 'F'로 시작하는 단어 필터링
                .print(); // 결과 출력

        // Flink 작업 실행
        env.execute("Flink Example Job");
    }
}
