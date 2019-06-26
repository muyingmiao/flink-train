package com.wxx.flink.course04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JavaCustomSinkToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("192.168.31.171", 9999);

        SingleOutputStreamOperator<Student> studenStream = source.map(new MapFunction<String, Student>() {
            public Student map(String s) throws Exception {
                String[] splits = s.split(",");
                Student stu = new Student();
                stu.setId(Integer.parseInt(splits[0]));
                stu.setName(splits[1]);
                stu.setAge(Integer.parseInt(splits[2]));
                return stu;
            }
        });

        studenStream.addSink(new SinkToMySQL());

        env.execute("JavaCustomSinkToMySQL");

    }


}
