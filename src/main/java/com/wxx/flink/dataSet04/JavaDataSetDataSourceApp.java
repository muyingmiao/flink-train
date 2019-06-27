package com.wxx.flink.dataSet04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

public class JavaDataSetDataSourceApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();

//        fromCollection(env);
//        textFile(env);
//        csvFilele(env);
//        readRecuriseFiles(env);
    }



    //递归文件夹机器下面的文档
    public static void readRecuriseFiles(ExecutionEnvironment env) throws Exception {
        String filePath =  "G:\\data\\flink\\nested";
        Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration", true);
        env.readTextFile(filePath).withParameters(parameters).print();
    }

    //读取csv文件
    public static void csvFile(ExecutionEnvironment env) throws Exception {
        String filePath =  "G:\\data\\flink\\people.csv";
        env.readCsvFile(filePath)
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .includeFields(true,true,true)
                .pojoType(Person.class, "name","age","work").print();
    }

    //读取文档类型的文件
    public static void textFile(ExecutionEnvironment env) throws Exception {
        String filePath = "G:\\data\\flink\\input.txt";
        env.readTextFile(filePath).print();
    }

    //读取集合类型的文件
    public static void fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for(int i=1; i<= 10; i++){
            list.add(i);
        }
        env.fromCollection(list).print();
    }
}
