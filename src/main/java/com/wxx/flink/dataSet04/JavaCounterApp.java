package com.wxx.flink.dataSet04;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

public class JavaCounterApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("hadoop","spark","flink","pyspark");

        DataSet<String> info = data.map(new RichMapFunction<String, String>() {
            LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("ele-counts-java", counter);
            }

            public String map(String s) throws Exception {
                counter.add(1);
                return s;
            }
        });

        String filepath= "G:\\data\\flink\\out-counter-java";
        info.writeAsText(filepath, FileSystem.WriteMode.OVERWRITE);

        JobExecutionResult jobResult = env.execute("JavaCounterApp");

        Long num = jobResult.getAccumulatorResult("ele-counts-java");
        System.out.println(num);
    }
}
