package com.sefonsoft.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hive.hcatalog.api.*;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HCatDemo {
    public HCatClient getHCatClient() throws Exception {
//        IMetaStoreClient hiveMetastoreClient = HCatUtil.getHiveMetastoreClient(new HiveConf());
//        hiveMetastoreClient.getAllDatabases();
        HCatClient hCatClient = HCatClient.create(new HiveConf());
        return hCatClient;
    }

    /**
     * 创建数据仓库
     *
     * @throws Exception
     */
    public void createDatabase() throws Exception {
        HCatClient hCatClient = getHCatClient();
        HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create("testjj").build();
        hCatClient.createDatabase(dbDesc);
    }

    /**
     * 查看数据仓库
     *
     * @throws Exception
     */
    @Test
    public void showDatabases() throws Exception {
        HCatClient hCatClient = getHCatClient();
        List<String> dbList = hCatClient.listDatabaseNamesByPattern("*");
        for (String db : dbList) {
            System.out.println(db);
        }
    }

    /**
     * 删除数据库
     *
     * @throws Exception
     */
    @Test
    public void deleteDatabase() throws Exception {
        HCatClient hCatClient = getHCatClient();
        hCatClient.dropDatabase("testjj", false, HCatClient.DropDBMode.CASCADE);
    }

    /**
     * 查看数据表
     *
     * @throws Exception
     */
    @Test
    public void showTables() throws Exception {
        HCatClient hCatClient = getHCatClient();
        List<String> tableList = hCatClient.listTableNamesByPattern("test", "*");
        for (String table : tableList) {
            System.out.println(table);
        }

    }

    @Test
    public void createTable01() throws Exception {
        HCatClient hCatClient = getHCatClient();
        List<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>();
        fieldSchemas.add(HCatFieldSchema.createMapTypeFieldSchema("id",new VarcharTypeInfo(),new HCatSchema(fieldSchemas),""));
        HCatTable hCatTable = new HCatTable("test","testhcaat").cols(fieldSchemas);
        HCatCreateTableDesc tableDesc = HCatCreateTableDesc.create(hCatTable).build();
        hCatClient.createTable(tableDesc);
    }


}
