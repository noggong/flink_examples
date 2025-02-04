package com.noggong.class2

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

class KotlinCode

fun main(args: Array<String>) {

    /**
     * 스트리밍 애플리케이션을 실행하는 데 필요한 컨텍스트를 생성합니다.
     * 데이터 소스(Source)와 싱크(Sink)를 정의하고, 스트리밍 데이터 흐름을 설정할 수 있도록 합니다.
     * execute() 메서드를 호출하여 데이터 파이프라인을 실제로 실행할 수 있도록 합니다.
     */
    // 1. 실행 환경 생성
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // 2. 소켓 데이터 소스 정의
    env.socketTextStream("localhost", 9999)
        // 3. 데이터 처리: 단어 나누기 및 (word, 1) 생성
        .flatMap(Tokenizer())
        // 4. 데이터 집계
        .keyBy { it.f0 }
        .sum(1)
        // 5. 데이터 싱크: 결과 출력
        .setParallelism(1) // 병렬도 제한
        .print()

    // 6. 작업 실행  : 실행잡의 이름
    env.execute("Flink Word Count Example")
}

class Tokenizer : FlatMapFunction<String, Tuple2<String, Int>> {
    override fun flatMap(
        p0: String,
        p1: Collector<Tuple2<String, Int>?>
    ) {
        p0.split(Regex("\\W+")).forEach { word ->
            if (word.isNotEmpty()) {
                p1.collect(Tuple2(word, 1))
            }
        }
    }
}