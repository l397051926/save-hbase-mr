package com.gennlife;

import com.gennlife.map.IndexHbaseMapper;
import com.gennlife.map.RWSHbaseMapper;
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

        //定义表名
        String  tableName="testPatient";

//        String input = "hdfs://192.168.187.21:9000/data/word/";       //本地
//        String input = "hdfs://10.0.2.21:9000/opt/test/data1.txt";      //测试
        String input = "hdfs://10.0.2.21:9000/user/hive/warehouse/gennlife_jszl_small.db/patient/";   //实际

        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum","192.168.187.21,192.168.187.22,192.168.187.23");        //本地
        conf.set("hbase.zookeeper.quorum","10.0.2.21,10.0.2.22,10.0.2.23");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("zookeeper.znode.parent","/hbase");

        Job job = Job.getInstance(conf,"RWSHBASE_JOB");

        job.setJarByClass(HbaseDriver.class);
        job.setMapperClass(RWSHbaseMapper.class);

        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,tableName);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path(input));

//        String dbname="gennlife_jszl_small";
//        String inputTable = "patient";
//        HCatInputFormat.setInput(job.getConfiguration(), dbname, inputTable);

        job.waitForCompletion(true);

//        /*---------------插入到index job------------*/
        //定义表名
        String indexName="testIndex";

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
