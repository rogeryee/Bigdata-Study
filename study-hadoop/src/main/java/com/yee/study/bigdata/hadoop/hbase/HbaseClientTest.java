package com.yee.study.bigdata.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Roger.Yi
 */
public class HbaseClientTest {

    private static final Logger logger = LoggerFactory.getLogger(HbaseClientTest.class);

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "rogermbp:2194");

        // 获取hbase连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        // Admin这个类主要用来创建表，删除表，启用禁用表等操作的接口类
        Admin admin = connection.getAdmin();

        /*
         *
         * HTableDescriptor 表描述信息的接口类
         * TableName 		描述表名称的接口类，把字符串（表名）变成hbase所认识的
         * HColumnDescriptor 列族的描述信息类，比如版本，压缩方式等等
         * Put				添加数据的时候需要用到，可以批量添加也可以单条添加
         * 					若是批量添加，需要创建一个list，将put对象放入
         * */
        HTableDescriptor hdc = new HTableDescriptor(TableName.valueOf("people"));
        HColumnDescriptor cf = new HColumnDescriptor("cinfo");
        cf.setMaxVersions(3);
        hdc.addFamily(cf); //添加列族

        //创建表
        if (!admin.tableExists(TableName.valueOf("people"))) {
            admin.createTable(hdc);
            logger.info("Table is created successfully.");
        } else {
            admin.disableTable(TableName.valueOf("people"));
            admin.deleteTable(TableName.valueOf("people"));
            logger.info("Table exist, and deleted successfully.");
        }
        //释放资源
        admin.close();
        logger.info("Operate table completed.");
    }
}
