package com.gennlife;

import com.gennlife.map.HbaseMapper;
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
        String  tableName="testTable";

//        String input = "hdfs://192.168.187.21:9000/data/word/";
//        String input = "hdfs://10.0.2.21:9000/opt/test/data.txt";
        String input = "hdfs://10.0.2.21:9000/user/hive/warehouse/gennlife_jszl_small.db/patient";
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum","192.168.187.21,192.168.187.22,192.168.187.23");
        conf.set("hbase.zookeeper.quorum","10.0.2.21,10.0.2.22,10.0.2.23");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("zookeeper.znode.parent","/hbase");

        Job job = Job.getInstance(conf,App.name);

        job.setJarByClass(HbaseDriver.class);
        job.setMapperClass(HbaseMapper.class);


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
    }

}
