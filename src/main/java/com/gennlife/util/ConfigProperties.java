package com.gennlife.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author liumingxin
 * @create 2018 15 18:13
 * @desc
 **/
public class ConfigProperties {
    /*zookeeper 相关配置*/
    public static  String HBASE_ZOOKEEPER_QUORUM;
    public static  String HBASE_ZOOKEEPERP_ROPERTY_CLIENTPORT;
    public static  String ZOOKEEPR_ZNODE_PARENT;

    /*Hbase 输出  相关配置*/
    public static  String RWS_TABLENAME;
    public static  String RWS_COLFAMILY1;
    public static  String RWS_COLFAMILY2;

    public static  String INDEXT_TABLENAME;
    public static  String INDEX_COLFAMILY1;
    public static  String INDEX_COLFAMILY2;


    /*hadoop 相关配置*/
    public static  String HADOOP_ADDRESS;

    /*hive 相关配置*/
    public static  String HIVE_DBNAME;
    public static  String HIVE_TABLENAME;

    /*压缩算法 snappy gzip */
    public static  String COMPRESS_NAME;

    private static Properties props=null;


    static {
        InputStream in = ConfigProperties.class.getClassLoader().getResourceAsStream("config.properties");
        props =new Properties();
        try {
            props.load(in);

            HBASE_ZOOKEEPER_QUORUM=props.getProperty("hbase_zookeeper_quorum");
            HBASE_ZOOKEEPERP_ROPERTY_CLIENTPORT=props.getProperty("hbase_zookeeperp_roperty_clientport");
            ZOOKEEPR_ZNODE_PARENT=props.getProperty("zookeepr_znode_parent");

            RWS_TABLENAME=props.getProperty("rws_tablename");
            RWS_COLFAMILY1=props.getProperty("rws_colfamily1");
            RWS_COLFAMILY2=props.getProperty("rws_colfamily2");

            INDEXT_TABLENAME=props.getProperty("indext_tablename");
            INDEX_COLFAMILY1=props.getProperty("index_colfamily1");
            INDEX_COLFAMILY2=props.getProperty("index_colfamily2");

             HADOOP_ADDRESS=props.getProperty("hadoop_address");

            HIVE_DBNAME=props.getProperty("hive_dbname");
            HIVE_TABLENAME=props.getProperty("hive_tablename");

            COMPRESS_NAME=props.getProperty("compress_name");

            in.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
