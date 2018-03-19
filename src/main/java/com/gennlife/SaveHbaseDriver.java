package com.gennlife;


import com.gennlife.map.IndexHbaseMapper;
import com.gennlife.map.RWSHbaseMapper;
import com.gennlife.util.ConfigProperties;
import com.gennlife.util.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * @author liumingxin
 * @create 2018 15 20:55
 * @desc  mr 入口 执行时可以采用带参数执行
 * 执行命令： hadoop jar xxx.jar db=[数据库名] table=[表名] rwst=[导出的hbase表名] indext=[导出的hbase表名]
 **/
public class SaveHbaseDriver extends Configured implements Tool {
    private static final Logger LOGGER= LoggerFactory.getLogger(SaveHbaseDriver.class);
    public int run(String[] strings) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        //获取 参数
        String[] args =new GenericOptionsParser(conf,strings).getRemainingArgs();
        Map<String,String> map =new HashMap<String, String>();
        // db=**** table=**** rwst=*** indext=****

        if(args.length>0 && args.length==4){
            for(int i=0;i<args.length;i++){
                map.put(args[i].split("=")[0],args[i].split("=")[1]);
            }
        }else {
            for(int i=0;i<args.length;i++){
                map.put(args[i].split("=")[0],args[i].split("=")[1]);
            }
            LOGGER.info("参数不正确， 正确参数格式为： db=**** table=**** rwst=*** indext=*** ，将未设置的参数自动设置为配置文件参数");
        }
        //定义要读的数据库名，以及表名
        String dbTable=map.get("db")==null? ConfigProperties.HIVE_DBNAME:map.get("db");
        String hTableName=map.get("table")==null?ConfigProperties.HIVE_TABLENAME:map.get("table");

        //定义表名
        String  tableName=map.get("rwst")==null?ConfigProperties.RWS_TABLENAME:map.get("rwst");
        //定义表名
        String indexName=map.get("indext")==null?ConfigProperties.INDEXT_TABLENAME:map.get("indext");
//        String input = "hdfs://192.168.187.21:9000/data/word/";       //本地
//        String input = "hdfs://10.0.2.21:9000/opt/test/data1.txt";      //测试
        String input = ConfigProperties.HADOOP_ADDRESS+"/"+dbTable+".db/"+hTableName+"/";   //实际

        //创建 Hbase 表
        HbaseUtils.creatRWSTable(tableName);
        HbaseUtils.createIndexTable(indexName);

//        conf.set("hbase.zookeeper.quorum","192.168.187.21,192.168.187.22,192.168.187.23");        //本地
        conf.set("hbase.zookeeper.quorum",ConfigProperties.HBASE_ZOOKEEPER_QUORUM);
        conf.set("hbase.zookeeper.property.clientPort",ConfigProperties.HBASE_ZOOKEEPERP_ROPERTY_CLIENTPORT);
        conf.set("zookeeper.znode.parent",ConfigProperties.ZOOKEEPR_ZNODE_PARENT);

        conf.set("mapreduce.map.memory.mb", "5000");
        conf.set("mapreduce.reduce.memory.mb", "5000");
        conf.set("mapreduce.map.java.opts", "-Xmx6000m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx6000m");
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "150000000");
        conf.set("mapreduce.input.fileinputformat.split.minsize", "67000000");
        conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", "134000000");
        conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", "134000000");

        /*---------------插入到RWS job------------*/

        Job job = Job.getInstance(conf,"RWSHBASE_JOB");

        job.setJarByClass(SaveHbaseDriver.class);
        job.setMapperClass(RWSHbaseMapper.class);

        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,tableName);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path(input));

        job.waitForCompletion(true);

        /*---------------插入到index job------------*/


        Job indexJob = Job.getInstance(conf,"INDEXHBASE_JOB");

        indexJob.setJarByClass(SaveHbaseDriver.class);
        indexJob.setMapperClass(IndexHbaseMapper.class);

        indexJob.setOutputFormatClass(TableOutputFormat.class);
        indexJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,indexName);
        indexJob.setOutputKeyClass(ImmutableBytesWritable.class);
        indexJob.setOutputValueClass(Put.class);

        indexJob.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(indexJob,new Path(input));

        return indexJob.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args){
        try {
            long startTime = System.currentTimeMillis();
            LOGGER.info("开始处理MR");
            //开始执行
            int exitCode = ToolRunner.run(new SaveHbaseDriver(),args);
            long endTime = System.currentTimeMillis();
            LOGGER.info("处理结束： 用时: "+(endTime-startTime)+"ms");
            System.exit(exitCode);
        }catch (Exception e){
            LOGGER.error(e.getMessage());
        }
    }

}
