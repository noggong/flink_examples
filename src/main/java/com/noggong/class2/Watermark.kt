import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.util.Collector
import java.time.Duration

fun main() {
    // Flink 실행 환경
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // 정적 데이터 소스
    val sourceData = listOf(
        Tuple2("user1", 1000L),
        Tuple2("user2", 2000L),
        Tuple2("user1", 3000L),
        Tuple2("user3", 3500L),
        Tuple2("user2", 5000L),
        Tuple2("user2", 2500L), // 지연 이벤트
        Tuple2("user1", 7000L)
    )

    // 워터마크를 설정한 스트림
    val streamWithWatermark: DataStream<Tuple2<String, Long>> = env.fromCollection(sourceData)
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .forBoundedOutOfOrderness<Tuple2<String, Long>>(Duration.ofSeconds(2)) // 최대 2초 지연 허용
                .withTimestampAssigner { event, _: Long -> event.f1 } // 이벤트 타임 설정
        )

    // 3초 텀블링 윈도우
    val windowedStream = streamWithWatermark
        .keyBy { it.f0 } // 사용자별 그룹화
        .window(TumblingEventTimeWindows.of(Time.seconds(3))) // 3초 윈도우
        .reduce(
            ReduceFunction { event1, event2 -> Tuple2(event1.f0, event1.f1 + event2.f1) }, // 이벤트 합산
            object : ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {
                override fun process(
                    key: String,
                    context: Context,
                    elements: MutableIterable<Tuple2<String, Long>>,
                    out: Collector<Tuple2<String, Long>>
                ) {
                    val result = elements.iterator().next() // ReduceFunction의 최종 결과 반환
                    out.collect(result)
                }
            }
        )

    // 결과 출력
    windowedStream.print("With Watermark")

    // Flink 실행
    env.execute("Watermark Example")
}