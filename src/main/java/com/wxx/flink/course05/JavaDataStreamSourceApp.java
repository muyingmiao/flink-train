package com.wxx.flink.course05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JavaDataStreamSourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        socketFunction(env);
//        nonParallelFunction(env);
//        parallelFuncton(env);
        richParallelFuncton(env);
        env.execute("JavaDataStreamSourceApp");
    }

    public static void richParallelFuncton(StreamExecutionEnvironment env){
        DataStreamSource<Long> data = env.addSource(new JavaCustomRichParallelFunction());
        data.print().setParallelism(1);
    }

    public static void parallelFuncton(StreamExecutionEnvironment env){
        DataStreamSource<Long> data = env.addSource(new JavaCustomParallelFunction());
        data.print().setParallelism(2);
    }


    public static void nonParallelFunction(StreamExecutionEnvironment env){
        DataStreamSource<Long> data = env.addSource(new JavaCustomNonParallelFunction());
        data.print();
    }

    public static void socketFunction(StreamExecutionEnvironment env){
       DataStreamSource<String> data =  env.socketTextStream("192.168.31.171", 9999);
       data.print().setParallelism(1);

    }
}
