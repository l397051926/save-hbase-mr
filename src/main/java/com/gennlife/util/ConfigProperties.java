package com.gennlife.util;

/**
 * @author liumingxin
 * @create 2018 15 18:13
 * @desc
 **/
public class ConfigProperties {
    /*zookeeper 相关配置*/
    public static final String HBASE_ZOOKEEPER_QUORUM="10.0.2.21,10.0.2.22,10.0.2.23";
    public static final String HBASE_ZOOKEEPERP_ROPERTY_CLIENTPORT="2181";
    public static final String ZOOKEEPR_ZNODE_PARENT="/hbase";

    /*Hbase  相关配置*/
    public static final String RWS_TABLENAME="testPatient";
    public static final String RWS_COLFAMILY1="patient_info";
    public static final String RWS_COLFAMILY2="visit_info";

    public static final String INDEXT_TABLENAME="testIndex";
    public static final String INDEX_COLFAMILY1="data";
    public static final String INDEX_COLFAMILY2="";

    /*hadoop 相关配置*/
    public static final String HADOOP_ADDRESS="hdfs://10.0.2.21:9000/user/hive/warehouse";

    /*hive 相关配置*/
    public static final String HIVE_DBNAME="gennlife_jszl_small";
    public static final String HIVE_TABLENAME="patient";

    /*压缩算法 snappy gzip */
    public static final String COMPRESS_NAME="snappy";
}
