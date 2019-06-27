package com.wxx.flink.dataSet04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class JavaDatasetSinkApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> list = new ArrayList<Integer>();
        for(int i = 0 ; i <= 10; i++){
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);
        String filepath = "G:\\data\\flink\\out_java" ;
        data.writeAsText(filepath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute();
    }
}
