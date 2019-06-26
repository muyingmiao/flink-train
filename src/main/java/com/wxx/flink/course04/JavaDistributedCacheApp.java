package com.wxx.flink.course04;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

public class JavaDistributedCacheApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "G:\\data\\flink\\input.txt";
        env.registerCachedFile(filePath, "local-java-doc");

        DataSource<String> data = env.fromElements("hadoop","spark","flink");

        data.map(new RichMapFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File file = getRuntimeContext().getDistributedCache().getFile("local-java-doc");
                List<String> lines = FileUtils.readLines(file);
                for(String line : lines){
                    System.out.println("line " + line);
                }
            }

            public String map(String s) throws Exception {
                return s;
            }
        }).print();

    }
}
