package com.sefonsfot.hbase.demo;

import java.io.IOException;

/**
 * 测试入口
 *
 * @author Junfeng
 */
public class Main {

    public static void main(String[] arg) throws IOException {
        HbaseDemo demo = new HbaseDemo("10.0.8.170,10.0.8.7", "2181", "/hbase-unsecure");
        demo.showTables("");
    }
}
