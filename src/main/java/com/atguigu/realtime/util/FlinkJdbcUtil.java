package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class FlinkJdbcUtil {
    public static Connection getPhoenixConnection() {
        return getJdbcConnection(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL, null, null);
    }

    public static Connection getJdbcConnection(String driver, String url, String user, String password) {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("错误，jdbc驱动错误！" + driver);
        }
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw new RuntimeException("错误！url=" + url + ",user=" + user + ",password=" + password);
        }
    }

    public static void closePhoenixConnection(Connection connection) {
        closeJdbcConnection(connection);
    }

    public static void closeJdbcConnection(Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("数据库连接关闭异常");
        }
    }
}
