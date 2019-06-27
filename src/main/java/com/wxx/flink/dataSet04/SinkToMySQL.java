package com.wxx.flink.dataSet04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkToMySQL extends RichSinkFunction<Student> {
    Connection connection;
    PreparedStatement pstmt;

    private Connection getConnection(){
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://192.168.31.171:3306/hive";
            conn = DriverManager.getConnection(url, "root", "root");
        }catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into student(id, name ,age) values(?,?,?)";
        pstmt = connection.prepareStatement(sql );
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(pstmt != null){
            pstmt.close();
        }
        if(connection != null){
            connection.close();
        }
    }

    public void invoke(Student value, Context context) throws Exception {
        pstmt.setInt(1, value.getId());
        pstmt.setString(2, value.getName());
        pstmt.setInt(3,value.getAge());

        pstmt.executeUpdate();
    }
}
