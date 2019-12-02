package com.sefonsoft.hive;


import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveDemo {

    /**
     *  获取Hive连接
     * @return hive连接
     * @throws Exception
     */
    public Connection getConnection() throws Exception {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        Class.forName(driverName);
        String url = "jdbc:hive2://sdc03.sefonsoft.com:2181,sdc01.sefonsoft.com:2181,sdc02.sefonsoft.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
        Connection connection = DriverManager.getConnection(url, "hive", "hive");
        return connection;
    }

    /**
     *  创建数据仓库
     * @throws Exception
     */
    public void createDatabase() throws Exception {
        Statement statement = getConnection().createStatement();
        statement.execute("create database testcli");
    }

    /**
     * 查看数据仓库
     * @throws Exception
     */
    @Test
    public void showDatabases() throws Exception {
        Statement statement = getConnection().createStatement();
        ResultSet resultSet = statement.executeQuery("show databases");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
    }

    /**
     *  删除数据仓库
     * @throws Exception
     */
    public void deleteDatabase() throws Exception {
        Statement statement = getConnection().createStatement();
        statement.execute("drop database testcli");
    }

    /**
     *  查看数据表
     * @throws Exception
     */
    public void showTables() throws Exception {
        Statement statement = getConnection().createStatement();
        ResultSet resultSet = statement.executeQuery("show tables in xj");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
    }

    /**
     * 创建数据表方式一
     * @throws Exception
     */
    public void createTable01() throws Exception {
        Statement statement = getConnection().createStatement();
        statement.execute("create table xj.dd(" +
                "id int," +
                "name string)" +
                "row format delimited fields terminated by '\t'");
    }

    /**
     * 创建数据表方式二
     * @throws Exception
     */
    public void createTable02() throws Exception {
        Statement statement = getConnection().createStatement();
        statement.execute("create table bb as select * from xj.class");
    }

    /**
     * 描述数据表
     * @throws Exception
     */
    public void desc() throws Exception {
        Statement statement = getConnection().createStatement();
        ResultSet rs = statement.executeQuery("desc formatted xj.class");
        while (rs.next()){
            System.out.println(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3));
        }
    }

    /**
     * 加载数据
     * @throws Exception
     */
    public void loadData() throws Exception {
        Statement statement = getConnection().createStatement();
        statement.execute("load data inpath '/test/dd.txt' into table xj.dd");
    }

    /**
     * 查询数据
     * @throws Exception
     */
    public void selectTable() throws Exception {
        Statement statement = getConnection().createStatement();
        ResultSet rs = statement.executeQuery("select * from xj.dd");
        while (rs.next()){
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }
    }

    /**
     * 删除数据表
     * @throws Exception
     */
    public void dropTable() throws Exception {
        Statement statement = getConnection().createStatement();
        statement.execute("drop table xj.dd");
    }
}
