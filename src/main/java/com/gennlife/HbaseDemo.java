package com.gennlife;

import com.gennlife.map.HbaseMapperNew;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

/**
 * @author liumingxin
 * @create 2018 15 11:00
 * @desc hive方式读入
 **/
//public class HbaseDemo  extends Configured implements Tool{
//    public int run(String[] args) throws Exception {
//        Configuration conf = HBaseConfiguration.create();
//
//        args = new GenericOptionsParser(conf,args).getRemainingArgs();
//
//        String tableName="testTable";
//        String dbName="gennlife_jszl_small";
//        String inputTableName ="patient";
//
//        conf.set("hbase.zookeeper.quorum","10.0.2.21,10.0.2.22,10.0.2.23");
//        conf.set("hbase.zookeeper.property.clientPort","2181");
//        conf.set("zookeeper.znode.parent","/hbase");
//
//        Job job = Job.getInstance(conf,"HbaseJob");
//
//        job.setJarByClass(HbaseDemo.class);
//        job.setMapperClass(HbaseMapperNew.class);
//
//        job.setOutputFormatClass(TableOutputFormat.class);
//        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,tableName);
//        job.setOutputKeyClass(ImmutableBytesWritable.class);
//        job.setOutputValueClass(Put.class);
//
//        job.setNumReduceTasks(0);
//
//        HCatInputFormat.setInput(job,dbName,inputTableName);
//        job.setInputFormatClass(HCatInputFormat.class);
//
//        return (job.waitForCompletion(true) ? 0 : 1);
//
//    }
//
//    public static void main(String[] args) throws Exception {
//        int exit = ToolRunner.run(new HbaseDemo(),args);
//        System.exit(exit);
//    }
//}
