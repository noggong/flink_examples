package com.noggong.class1;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

// 차량 속도 데이터의 POJO 클래스
class SpeedEvent {
    public String vehicleId; // 차량 ID
    public long timestamp;   // 이벤트 발생 시간 (밀리초)
    public double speed;     // 차량 속도 (km/h)

    // 생성자
    public SpeedEvent(String vehicleId, long timestamp, double speed) {
        this.vehicleId = vehicleId;
        this.timestamp = timestamp;
        this.speed = speed;
    }
}

public class SpeedingViolation {
    public static void main(String[] args) throws Exception {
        // Flink 실행 환경 생성
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 활성화 (10초 주기)
        env.enableCheckpointing(10000); // 10초마다 Checkpoint 생성
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 정확히 한 번 처리 보장
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints")); // Checkpoint 저장 경로 설정

        // 차량 속도 데이터를 시뮬레이션하는 입력 데이터 스트림
        DataStream<SpeedEvent> speedStream = env.fromElements(
                new SpeedEvent("A1", 1675000000000L, 80.0), // 차량 A1의 이벤트 (80km/h)
                new SpeedEvent("B2", 1675000005000L, 120.0), // 차량 B2의 이벤트 (120km/h)
                new SpeedEvent("A1", 1675000010000L, 95.0),  // 차량 A1의 이벤트 (95km/h)
                new SpeedEvent("B2", 1975000020000L, 135.0)  // 차량 B2의 이벤트 (135km/h)
        ).assignTimestampsAndWatermarks(
                // 워터마크를 생성하고 이벤트 시간 설정 (지연 허용 2초)
                WatermarkStrategy.<SpeedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp) // 이벤트 시간 추출기
        );

        // 이벤트 시간 기반 윈도우 처리 (10초 단위)
        DataStream<String> windowedStream = speedStream
                .keyBy(event -> event.vehicleId) // 차량 ID로 데이터를 그룹화
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 10초 윈도우 생성
                .trigger(EventTimeTrigger.create()) // 이벤트 시간 기반 트리거 사용
                .process(new ProcessWindowFunction<SpeedEvent, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<SpeedEvent> elements, Collector<String> out) {
                        int count = 0;
                        double totalSpeed = 0.0;

                        for (SpeedEvent event : elements) { // 윈도우 내 모든 이벤트 순회
                            count++;
                            totalSpeed += event.speed; // 속도 합산
                        }

                        double averageSpeed = totalSpeed / count; // 평균 속도 계산
                        out.collect("차량 ID: " + key + ", 평균 속도: " + averageSpeed);
                    }
                });

        // 과속 차량 감지를 위한 상태 기반 처리
        DataStream<String> overSpeedAlerts = speedStream
                .keyBy(event -> event.vehicleId) // 차량 ID로 데이터를 그룹화
                .process(new KeyedProcessFunction<String, SpeedEvent, String>() {
                    private transient ValueState<Double> maxSpeedState; // 상태: 최대 속도

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                        // 최대 속도 상태를 초기화
                        maxSpeedState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("maxSpeed", Double.class)
                        );
                    }

                    @Override
                    public void processElement(SpeedEvent event, Context ctx, Collector<String> out) throws Exception {
                        // 현재 저장된 최대 속도 가져오기
                        Double currentMaxSpeed = maxSpeedState.value();
                        if (currentMaxSpeed == null || event.speed > currentMaxSpeed) {
                            maxSpeedState.update(event.speed); // 새로운 최대 속도 저장
                        }

                        // 과속 기준 (100km/h 이상일 경우)
                        if (event.speed > 100.0) {
                            out.collect("[과속 경고] 차량 ID: " + event.vehicleId + ", 속도: " + event.speed);
                        }
                    }
                });

        // 결과 출력 (윈도우 처리 결과 및 과속 경고)
        windowedStream.print(); // 평균 속도 출력
        overSpeedAlerts.print(); // 과속 경고 출력

        // 플링크 작업 실행
        env.execute("Flink Stream Processing Example with Checkpointing");
    }
}