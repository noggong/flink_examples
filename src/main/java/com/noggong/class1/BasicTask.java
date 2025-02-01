package com.noggong.class1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * terminal 에서 socket 연결
 * $ nc -lk 9999
 */
public class BasicTask {

    public static void main(String[] args) throws Exception {
        // 1. 실행 환경 생성
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 소켓 데이터 소스 정의
        env.socketTextStream("localhost", 9999)
                // 3. 데이터 처리: 단어 나누기 및 (word, 1) 생성
                .flatMap(new Tokenizer())
                // 4. 데이터 집계
                .keyBy(value -> value.f0)
                .sum(1)
                // 5. 데이터 싱크: 결과 출력
                .setParallelism(1) // 병렬도 제한
                .print();

        // 6. 작업 실행
        env.execute("Flink Word Count Example");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String word : value.split("\\W+")) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
