package com.noggong.class2

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import java.time.Duration

class WindowOperator

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // 정적 데이터 소스 정의
    val sourceData = listOf(
        Tuple2("user1", 1), Tuple2("user2", 1), Tuple2("user1", 1),
        Tuple2("user3", 1), Tuple2("user2", 1), Tuple2("user1", 1)
    )
    val stream: DataStream<Tuple2<String, Int>> = env.fromCollection(sourceData)
        .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness<Tuple2<String, Int>>(Duration.ofSeconds(1)))

    // 세션 윈도우 연산 (5초 세션 간격)
    val sessionWindow = stream
        .keyBy { it.f0 }
        .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
        .reduce(ReduceFunction { a, b -> Tuple2(a.f0, a.f1 + b.f1) })

    // 변환된 데이터 출력
    sessionWindow.print("Session Window")

    env.execute("Window Operations Example")
}
