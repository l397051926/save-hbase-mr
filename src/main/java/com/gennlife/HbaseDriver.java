package com.gennlife;

import com.gennlife.map.IndexHbaseMapper;
import com.gennlife.map.RWSHbaseMapper;
import com.gennlife.util.ConfigProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.IOException;

/**
 * @author liumingxin
 * @create 2018 14 11:03
 * @desc
 **/
public class HbaseDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length<4){
            args=new String[4];
        }

        //定义要读的数据库名，以及表名
        String idTable=args[0]==null?ConfigProperties.HIVE_DBNAME:args[0];
        String hTableName=args[1]==null?ConfigProperties.HIVE_TABLENAME:args[1];

        //定义表名
        String  tableName=args[2]==null?ConfigProperties.RWS_TABLENAME:args[0];
        //定义表名
        String indexName=args[3]==null?ConfigProperties.INDEXT_TABLENAME:args[3];
//        String input = "hdfs://192.168.187.21:9000/data/word/";       //本地
//        String input = "hdfs://10.0.2.21:9000/opt/test/data1.txt";      //测试
        String input = ConfigProperties.HADOOP_ADDRESS+"/"+idTable+".db/"+hTableName+"/";   //实际

        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum","192.168.187.21,192.168.187.22,192.168.187.23");        //本地
        conf.set("hbase.zookeeper.quorum",ConfigProperties.HBASE_ZOOKEEPER_QUORUM);
        conf.set("hbase.zookeeper.property.clientPort",ConfigProperties.HBASE_ZOOKEEPERP_ROPERTY_CLIENTPORT);
        conf.set("zookeeper.znode.parent",ConfigProperties.ZOOKEEPR_ZNODE_PARENT);

        Job job = Job.getInstance(conf,"RWSHBASE_JOB");

        job.setJarByClass(HbaseDriver.class);
        job.setMapperClass(RWSHbaseMapper.class);

        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,tableName);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path(input));

        job.waitForCompletion(true);

//        /*---------------插入到index job------------*/


        Job indexJob = Job.getInstance(conf,"INDEXHBASE_JOB");

        indexJob.setJarByClass(HbaseDriver.class);
        indexJob.setMapperClass(IndexHbaseMapper.class);

        indexJob.setOutputFormatClass(TableOutputFormat.class);
        indexJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,indexName);
        indexJob.setOutputKeyClass(ImmutableBytesWritable.class);
        indexJob.setOutputValueClass(Put.class);

        indexJob.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(indexJob,new Path(input));

        indexJob.waitForCompletion(true);

    }

}
