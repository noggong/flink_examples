package com.noggong.class2

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class RollingOperator

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val sourceData = listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val stream = env.fromCollection(sourceData)

    val rollingSumStream = stream.keyBy { _ -> 1 }
        .reduce(object : ReduceFunction<Int> {
            override fun reduce(value1: Int, value2: Int): Int {
                return value1 + value2
            }
        })

    // 변환된 데이터 출력
    rollingSumStream.print()
    env.execute("Flink Rolling Sum Example")
}