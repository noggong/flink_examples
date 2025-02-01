package com.noggong.class1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TimerExample {
    public static void main(String[] args) throws Exception {
        // 1. Flink 실행 환경 생성
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements("Hello", "World", "Flink") // 데이터 소스
                .keyBy(value -> value) // KeyedStream 생성
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        // 5초 후 타이머 설정
                        long triggerTime = ctx.timerService().currentProcessingTime() + 5000;
                        ctx.timerService().registerProcessingTimeTimer(triggerTime);

                        // 결과 출력
                        out.collect("Registered Timer for: " + value);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 타이머 트리거 시 동작
                        out.collect("Timer triggered for key: " + ctx.getCurrentKey());
                    }
                })
                .print(); // 결과 출력

        // 2. Flink 작업 실행
        env.execute("Timer Example");
    }
}
