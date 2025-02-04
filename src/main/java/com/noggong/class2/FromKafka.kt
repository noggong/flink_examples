//package com.noggong.class2
//
//import org.apache.flink.api.common.functions.MapFunction
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import java.util.Properties
//
//fun main() {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment()
//
//    // Kafka에서 데이터를 가져오는 데이터 소스 예제
//    val properties = Properties().apply {
//        setProperty("bootstrap.servers", "localhost:9092")
//        setProperty("group.id", "flink-group")
//    }
//
//    val kafkaConsumer = FlinkKafkaConsumer("flink-topic", SimpleStringSchema(), properties)
//    val stream: DataStream<String> = env.addSource(kafkaConsumer)
//
//    // 데이터 변환 예제: 문자열을 대문자로 변환
//    val transformedStream = stream.map(object : MapFunction<String, String> {
//        override fun map(value: String): String {
//            return value.uppercase()
//        }
//    })
//
//    // 변환된 데이터 출력
//    transformedStream.print()
//
//    env.execute("Flink Kafka Streaming Example")
//}
