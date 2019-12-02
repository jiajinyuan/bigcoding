package com.sefonsfot.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

public class HBaseApi {


    /**
     * 获取Hbase连接
     *
     * @return Hbase连接
     * @throws IOException
     */
    public Connection getConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum.", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        Connection conn = ConnectionFactory.createConnection(conf);
        return conn;
    }

    /**
     * 查看Namespace
     *
     * @throws IOException
     */
    public void showNamespaces() throws IOException {
        // 获取Admin
        Admin admin = getConnection().getAdmin();
        // 获取Namespace描述
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        // 遍历获取Namespace名
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println(namespaceDescriptor.getName());
        }
    }

    /**
     * 创建Namespace
     *
     * @throws IOException
     */
    public void createNamespace() throws IOException {
        // 获取Admin
        Admin admin = getConnection().getAdmin();
        // 创建Namespace
        admin.createNamespace(NamespaceDescriptor.create("nstest").build());
    }

    /**
     * 删除Namespace
     *
     * @throws IOException
     */
    public void deleteNamespace() throws IOException {
        // 获取Admin
        Admin admin = getConnection().getAdmin();
        // 删除Namespace
        admin.deleteNamespace("nstest");
    }

    /**
     * 查看表
     *
     * @throws IOException
     */
    public void showTables() throws IOException {
        // 获取Admin
        Admin admin = getConnection().getAdmin();
        // 通过正则表达式获取Namespace为nstest的表
        TableName[] tableNames = admin.listTableNames("nstest:.*");
        // 遍历获取表名
        for (TableName tableName : tableNames) {
            System.out.println(new String(tableName.getName()));
        }
    }

    /**
     * 创建表
     *
     * @throws IOException
     */
    public void createTable(List<String> familyList) throws IOException {
        // 获取Admin
        Admin admin = getConnection().getAdmin();
        // 创建表描述
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("nstest:stu"));
        // 添加至少一个列簇
        hTableDescriptor.addFamily(new HColumnDescriptor("basocinfo"));
        hTableDescriptor.addFamily(new HColumnDescriptor("extendinfo"));
        // 创建表
        admin.createTable(hTableDescriptor);
    }

    /**
     * 删除表
     *
     * @throws IOException
     */
    public void deleteTable() throws IOException {
        // 获取Admin
        Admin admin = getConnection().getAdmin();
        // 禁用表
        admin.disableTable(TableName.valueOf("nstest:stu"));
        // 删除表
        admin.deleteTable(TableName.valueOf("nstest:stu"));
    }

    /**
     * 添加数据
     *
     * @throws IOException
     */
    public void putData() throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf("nstest:stu"));
        // 根据rowkey获取Put
        Put put = new Put("000001".getBytes());
        // 设置列簇，列，值
        put.addColumn("info".getBytes(), "name".getBytes(), "tom".getBytes());
        // 添加数据
        table.put(put);
    }

    /**
     * 描述表
     *
     * @throws IOException
     */
    public void desc() throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf("nstest:stu"));
        // 获取表描述
        HTableDescriptor tableDescriptor = table.getTableDescriptor();
        // 获取列簇描述
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        // 获取列簇名
        for (HColumnDescriptor columnFamily : columnFamilies) {
            System.out.println(new String(columnFamily.getName()));
        }
    }

    /**
     * 获取数据
     *
     * @throws IOException
     */
    public void getData() throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf("nstest:stu"));
        // 根据rowkey获取Get
        Get get = new Get("000001".getBytes());
        Result result = table.get(get);
        // 获取Cell
        List<Cell> cells = result.listCells();
        // 遍历Cell -> rowkey family column value
        for (Cell cell : cells) {
            System.out.println(new String(CellUtil.cloneRow(cell)) + "\t" + new String(CellUtil.cloneFamily(cell))
                    + ":" + new String(CellUtil.cloneQualifier(cell)) + "\t" + new String(CellUtil.cloneValue(cell)));
        }
    }

    /**
     * 表扫描
     *
     * @throws IOException
     */
    public void scan() throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf("nstest:stu"));
        // 获取扫描器
        Scan scan = new Scan();
        // 扫描
        ResultScanner scanner = table.getScanner(scan);
        List<Cell> cells = scanner.next().listCells();
        // 遍历Cell -> rowkey family:column   value
        for (Cell cell : cells) {
            System.out.println(new String(CellUtil.cloneRow(cell)) + "\t" + new String(CellUtil.cloneFamily(cell))
                    + ":" + new String(CellUtil.cloneQualifier(cell)) + "\t" + new String(CellUtil.cloneValue(cell)));
        }
    }

    /**
     * 删除数据
     *
     * @throws IOException
     */
    public void deleteData() throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf("nstest:stu"));
        // 根据rowkey删除数据
        Delete delete = new Delete("000001".getBytes());
        // 删除列簇为info的数据
        delete.addFamily("info".getBytes());
        table.delete(delete);
    }
}

