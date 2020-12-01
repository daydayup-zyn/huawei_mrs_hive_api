package com.zhbr.cqljgt.demo.hive;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName ConnHuaweiHive
 * @Description TODO
 * @Autor yanni
 * @Date 2020/11/24 9:57
 * @Version 1.0
 **/
public class ConnHuaweiHive {


    private static Connection connection = null;
    private static PreparedStatement statement = null;
    private static ResultSet resultSet = null;
    private static List<Map<String, Object>> list= null;
    private static String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    static {
        try {
            Class.forName(HIVE_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static List<Map<String, Object>> runSelect(String url, String userName,String password,String sql){
        try {
            connection = DriverManager.getConnection(url);
            statement = connection.prepareStatement(sql);
            resultSet = statement.executeQuery();
            list = selectResultSetToList(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static List<Map<String, Object>> runDesc(String url, String userName,String password,String sql){
        try {
            connection = DriverManager.getConnection(url);
            statement = connection.prepareStatement(sql);
            resultSet = statement.executeQuery();
            list = descResultSetToList(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static void set(String url,String sql){
        try {
            connection = DriverManager.getConnection(url);
            statement = connection.prepareStatement(sql);
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static List<Map<String, Object>> selectResultSetToList(ResultSet rs) {
        String newCloName = "";
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        try {
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            while (rs.next()) {
                Map<String, Object> rowData = new HashMap<String, Object>();
                for (int i = 1; i <= columnCount; i++) {
                    if (md.getColumnName(i).contains(".")){
                        newCloName = md.getColumnName(i).split("\\.")[1];
                    }else {
                        newCloName = md.getColumnName(i);
                    }
                    rowData.put(newCloName, rs.getObject(i));
                }
                list.add(rowData);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                rs = null;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return list;
    }


    public static List<Map<String, Object>> descResultSetToList(ResultSet rs) {
        String newCloName = "";
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        try {
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            while (rs.next()) {
                Map<String, Object> rowData = new HashMap<String, Object>();
                for (int i = 1; i <= columnCount; i++) {
                    newCloName = md.getColumnName(i);
                    if (newCloName.equals("col_name")){
                        rowData.put("name", rs.getObject(i));
                    }else if(newCloName.equals("data_type")){
                        rowData.put("type", rs.getObject(i));
                    }else if(newCloName.equals("comment")){
                        rowData.put("cname", rs.getObject(i));
                    }
                }
                list.add(rowData);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                rs = null;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return list;
    }
}
