package com.noggong.class1;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedStateExample {
    public static void main(String[] args) throws Exception {
        // 1. Flink 실행 환경 생성
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 데이터 스트림 정의 (KeyedStream 생성)
        env.fromElements(
                        Tuple2.of("user1", 5), // 데이터 예시 (사용자 "user1", 값 5)
                        Tuple2.of("user2", 3), // 사용자 "user2", 값 3
                        Tuple2.of("user1", 2), // 사용자 "user1", 값 2
                        Tuple2.of("user2", 4)  // 사용자 "user2", 값 4
                )
                .keyBy(value -> value.f0) // Key 기준 (user ID)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, String>() {
                    // 상태를 저장할 ValueState 정의
                    private transient ValueState<Integer> sumState;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) {
                        // 상태 초기화 (ValueState는 key마다 별도로 유지됨)
                        sumState = getRuntimeContext().getState(new ValueStateDescriptor<>("sumState", Integer.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        // 현재 상태값 읽기
                        Integer currentSum = sumState.value();
                        if (currentSum == null) {
                            currentSum = 0; // 상태가 null이면 초기화
                        }

                        // 새로운 값을 기존 합계에 더함
                        currentSum += value.f1;

                        // 상태 업데이트
                        sumState.update(currentSum);

                        // 결과 출력
                        out.collect("User: " + value.f0 + ", Total: " + currentSum);
                    }
                })
                .print(); // 최종 결과 출력

        // 3. Flink 작업 실행
        env.execute("Keyed State Example");
    }
}
