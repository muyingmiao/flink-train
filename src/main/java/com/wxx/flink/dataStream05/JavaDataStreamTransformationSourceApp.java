package com.wxx.flink.dataStream05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class JavaDataStreamTransformationSourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        parallelFuncton(env);
//        unionFuncton(env);
        splitSelectFuncton(env);
        env.execute("JavaDataStreamTransformationSourceApp");
    }

    public static void splitSelectFuncton(StreamExecutionEnvironment env){
        DataStreamSource<Long> data = env.addSource(new JavaCustomNonParallelFunction());
        SplitStream<Long> splits =  data.split(new OutputSelector<Long>() {
            public Iterable<String> select(Long aLong) {
                List<String> output = new ArrayList<String>();
                if(aLong % 2 ==0){
                    output.add("even");
                }else{
                    output.add("odd");
                }
                return output;
            }
        });
        splits.select("odd").print().setParallelism(1);
    }


    public static void unionFuncton(StreamExecutionEnvironment env){
        DataStreamSource<Long> data1= env.addSource(new JavaCustomParallelFunction());
        DataStreamSource<Long> data2 = env.addSource(new JavaCustomParallelFunction());
        data1.union(data2).print().setParallelism(1);
    }

    public static void parallelFuncton(StreamExecutionEnvironment env){
        DataStreamSource<Long> data = env.addSource(new JavaCustomParallelFunction());
        data.map(new MapFunction<Long, Long>() {
            public Long map(Long value) throws Exception {
                return value;
            }
        }).filter(new FilterFunction<Long>() {
            public boolean filter(Long value) throws Exception {
                return value %2 == 0;
            }
        }).print().setParallelism(2);
    }

}
