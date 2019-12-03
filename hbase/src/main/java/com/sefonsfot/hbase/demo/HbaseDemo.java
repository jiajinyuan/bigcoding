package com.sefonsfot.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

public class HbaseDemo {

    private String quorum;
    private String port;
    private String parent;

    public HbaseDemo(String quorum, String port, String parent) {
        this.quorum = quorum;
        this.port = port;
        this.parent = parent;
    }

    /**
     * 获取Hbase连接
     *
     * @return Hbase连接
     * @throws IOException
     */
    public Connection getConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.property.clientPort", port);
        conf.set("zookeeper.znode.parent", parent);
        return ConnectionFactory.createConnection(conf);
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
     * @param namespace Namespace名称
     * @throws IOException
     */
    public void createNamespace(String namespace) throws IOException {
        // 获取Admin
        Admin admin = getConnection().getAdmin();
        // 创建Namespace
        admin.createNamespace(NamespaceDescriptor.create(namespace).build());
    }

    /**
     * 删除Namespace
     *
     * @param namespace Namespace名称
     * @throws IOException
     */
    public void deleteNamespace(String namespace) throws IOException {
        // 获取Admin
        Admin admin = getConnection().getAdmin();
        // 删除Namespace
        admin.deleteNamespace(namespace);
    }

    /**
     * 查看表(通过正则表达式)
     *
     * @param regex 写法namespace:.*  -> eg:  nstest:.*
     * @throws IOException
     */
    public void showTables(String regex) throws IOException {
        // 获取Admin
        Admin admin = getConnection().getAdmin();
        // 获取符合规则的TableName
        TableName[] tableNames;
        if (null == regex || "".equals(regex)) {
            tableNames = admin.listTableNames();
        } else {
            tableNames = admin.listTableNames(regex);
        }
        // 遍历获取表名
        for (TableName tableName : tableNames) {
            System.out.println(new String(tableName.getName()));
        }
        admin.close();
    }

    /**
     * 创建表
     *
     * @param namespaceTable 写法:  nstest:data
     * @param familyList     列簇集合
     * @throws IOException
     */
    public void createTable(String namespaceTable, List<String> familyList) throws IOException {
        // 获取Admin
        Admin admin = getConnection().getAdmin();
        // 创建表描述
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(namespaceTable));
        // 添加至少一个列簇,familyList大小至少为一
        for (String family : familyList) {
            hTableDescriptor.addFamily(new HColumnDescriptor(family));
        }
        // 创建表
        admin.createTable(hTableDescriptor);
    }

    /**
     * 删除表
     *
     * @param namespaceTable 写法:  nstest:data
     * @throws IOException
     */
    public void deleteTable(String namespaceTable) throws IOException {
        // 获取Admin
        Admin admin = getConnection().getAdmin();
        // 禁用表
        admin.disableTable(TableName.valueOf(namespaceTable));
        // 删除表
        admin.deleteTable(TableName.valueOf(namespaceTable));
    }

    /**
     * 添加数据
     *
     * @param namespaceTable 写法:  nstest:data
     * @param rowkey         rowkey
     * @param family         列簇
     * @param column         列
     * @param value          值
     * @throws IOException
     */
    public void putData(String namespaceTable, String rowkey, String family, String column, String value) throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf(namespaceTable));
        // 获取Put
        Put put = new Put(rowkey.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
        // 添加数据
        table.put(put);
    }

    /**
     * 描述表
     *
     * @param namespaceTable 写法:  nstest:data
     * @throws IOException
     */
    public void desc(String namespaceTable) throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf(namespaceTable));
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
     * @param namespaceTable 写法:  nstest:data
     * @param rowkey         rowkey
     * @throws IOException
     */
    public void getData(String namespaceTable, String rowkey) throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf(namespaceTable));
        // 获取Get
        Get get = new Get(rowkey.getBytes());
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
     * @param namespaceTable 写法:  nstest:data
     * @throws IOException
     */
    public void scan(String namespaceTable) throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf(namespaceTable));
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
     * @param namespaceTable 写法:  nstest:data
     * @param rowkey         rowkey
     * @throws IOException
     */
    public void deleteData(String namespaceTable, String rowkey) throws IOException {
        // 获取连接
        Connection connection = getConnection();
        // 获取表
        Table table = connection.getTable(TableName.valueOf(namespaceTable));
        // 根据rowkey删除数据
        Delete delete = new Delete(rowkey.getBytes());
        table.delete(delete);
    }
}
