package com.noggong.class1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

// 건희님 코드 예제
@SuppressWarnings("deprecation")
public class WordCountStreamProcess {
    public static void main(String[] args) throws Exception {
        // 1. init context and env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 배치처리 위한 2가지 방법
        // 1. runtime module 변경
        // 2. bin/flink run --execution.runtime-mode BATCH
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 2. source define
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/words.txt");// deprecated 이지만 설명을 위해

        // 3. transformation
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndCount = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                Arrays.stream(line.split("\\s+")).forEach(
                        word -> collector.collect(Tuple2.of(word, 1L))
                );
            }
        });

        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordAndCount.keyBy(tuple -> tuple.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);
        sum.print();

        env.execute(); // DataSet API와의 차이점
    }
}
