package com.wxx.flink.timeAndWindow07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class JavaWindowsProcessApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("192.168.31.171",9999);
        text.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                String[] tokens = s.toLowerCase().split(",");
                for(String token: tokens){
                    if(token.length() > 0){
                        collector.collect(new Tuple2<Integer, Integer>(1, Integer.parseInt(token)));
                    }
                }
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<Integer,Integer>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Integer>> iterable, Collector<Object> collector) throws Exception {
                        long count = 0;
                        for(Tuple2<Integer, Integer> in : iterable){
                            count ++;
                        }
                        collector.collect("window:" + context.window() + "count: " + count);
                    }
                })
                .print()
                .setParallelism(1);
        env.execute("JavaWindowsReduceApp");
    }
}
