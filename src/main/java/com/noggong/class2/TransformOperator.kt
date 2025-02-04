package com.noggong.class2
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class BasicOperator

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // 독립적으로 실행할 수 있는 데이터 소스 예제
    val sourceData = listOf("Flink", "Streaming", "Processing", "Example")
    val stream: DataStream<String> = env.fromCollection(sourceData)

    // 변환 연산 예제: 각 문자열의 길이를 계산
    val transformedStream = stream.map(object : MapFunction<String, Int> {
        override fun map(value: String): Int {
            return value.length
        }
    })

    // 변환된 데이터 출력
    transformedStream.print()

    env.execute("Flink Transformation Example")
}
