package com.gennlife;

import com.gennlife.map.IndexHbaseMapper;
import com.gennlife.map.RWSHbaseMapper;
import com.gennlife.util.ConfigProperties;
import com.gennlife.util.HbaseUtils;
import com.gennlife.util.TimesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
 * @desc mr 入口 执行时可以采用带参数执行
 **/
public class SaveHbaseBulkloadDriver extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(SaveHbaseBulkloadDriver.class);

    public int run(String[] strings) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        //获取 参数
        String[] args = new GenericOptionsParser(conf, strings).getRemainingArgs();
        Map<String, String> map = new HashMap<String, String>();
        // db=**** table=**** rwst=*** indext=****

        if (args.length > 0 && args.length == 4) {
            for (int i = 0; i < args.length; i++) {
                map.put(args[i].split("=")[0], args[i].split("=")[1]);
            }
        } else {
            for (int i = 0; i < args.length; i++) {
                map.put(args[i].split("=")[0], args[i].split("=")[1]);
            }
            LOGGER.info("参数不正确，正确参数格式为： db=**** table=**** rwst=*** indext=*** ，将未设置的参数自动设置为配置文件参数");
        }
        //定义要读的数据库名，以及表名
        String dbTable = map.get("db") == null ? ConfigProperties.HIVE_DBNAME : map.get("db");
        String hTableName = map.get("table") == null ? ConfigProperties.HIVE_TABLENAME : map.get("table");

        //定义表名
        String tableName = map.get("rwst") == null ? ConfigProperties.RWS_TABLENAME : map.get("rwst");
        //定义表名
        String indexName = map.get("indext") == null ? ConfigProperties.INDEXT_TABLENAME : map.get("indext");

        String inputPath = ConfigProperties.HADOOP_ADDRESS + "/" + dbTable + ".db/" + hTableName + "/";   //实际
        String outputPathRWS = "hdfs://10.0.2.21:9000/opt/hbase/output/rws/"+tableName;
        String outputPathIndex = "hdfs://10.0.2.21:9000/opt/hbase/output/index/"+indexName;

        Path pathRWS = new Path(outputPathRWS);
        Path pathIndxe = new Path(outputPathIndex);

        //如果源文件存在则删除
        FileSystem fileSystemRWS = pathRWS.getFileSystem(conf);
        FileSystem fileSystemIndex = pathIndxe.getFileSystem(conf);
        if (fileSystemRWS.exists(pathRWS)) {
            fileSystemRWS.delete(pathRWS, true);
            LOGGER.info(outputPathRWS+"目录存在 ----删除该目录");
        }
        if (fileSystemIndex.exists(pathIndxe)) {
            fileSystemIndex.delete(pathIndxe, true);
            LOGGER.info(outputPathIndex+"目录存在 ----删除该目录");
        }

        //创建 Hbase 表
        HbaseUtils.creatRWSTable(tableName);
        HbaseUtils.createIndexTable(indexName);

        conf.set("mapreduce.map.memory.mb", "5000");
        conf.set("mapreduce.reduce.memory.mb", "5000");
        conf.set("mapreduce.map.java.opts", "-Xmx6000m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx6000m");
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "150000000");
        conf.set("mapreduce.input.fileinputformat.split.minsize", "67000000");
        conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", "134000000");
        conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", "134000000");


        HTable hTableRws = null;
        HTable hTableIndex = null;
        try {
            /*---------------------rws job-----------*/
            Job RWSjob = Job.getInstance(conf, "RWSHBASE_JOB");
            RWSjob.setJarByClass(SaveHbaseBulkloadDriver.class);
            RWSjob.setMapperClass(RWSHbaseMapper.class);
            RWSjob.setMapOutputKeyClass(ImmutableBytesWritable.class);
            RWSjob.setMapOutputValueClass(Put.class);
            // speculation
            RWSjob.setSpeculativeExecution(false);
            RWSjob.setReduceSpeculativeExecution(false);
            // in/out format
//            job.setInputFormatClass(FileInputFormat.class);
            RWSjob.setOutputFormatClass(HFileOutputFormat2.class);

            FileInputFormat.setInputPaths(RWSjob, new Path(inputPath));
            FileOutputFormat.setOutputPath(RWSjob, new Path(outputPathRWS));

            /*------------------------index job--------------*/

            Job indexjob = Job.getInstance(conf, "INDEXHBASE_JOB");
            indexjob.setJarByClass(SaveHbaseBulkloadDriver.class);
            indexjob.setMapperClass(IndexHbaseMapper.class);
            indexjob.setMapOutputKeyClass(ImmutableBytesWritable.class);
            indexjob.setMapOutputValueClass(Put.class);
            // speculation
            indexjob.setSpeculativeExecution(false);
            indexjob.setReduceSpeculativeExecution(false);
            // in/out format
            indexjob.setOutputFormatClass(HFileOutputFormat2.class);

            FileInputFormat.setInputPaths(indexjob, new Path(inputPath));
            FileOutputFormat.setOutputPath(indexjob, new Path(outputPathIndex));


            hTableRws = new HTable(conf, tableName);
            hTableIndex = new HTable(conf, indexName);

            HFileOutputFormat2.configureIncrementalLoad(RWSjob, hTableRws);
            HFileOutputFormat2.configureIncrementalLoad(indexjob, hTableIndex);


            if (RWSjob.waitForCompletion(true)) {
                FsShell shell = new FsShell(conf);
                try {
                    shell.run(new String[]{"-chmod", "-R", "777", outputPathRWS});
                } catch (Exception e) {
                    LOGGER.error("Couldnt change the file permissions ", e);

                }
                //载入到hbase表
                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
                loader.doBulkLoad(new Path(outputPathRWS), hTableRws);
            } else {
                LOGGER.error("loading failed.");
            }

            if (indexjob.waitForCompletion(true)) {
                FsShell shell = new FsShell(conf);
                try {
                    shell.run(new String[]{"-chmod", "-R", "777", outputPathIndex});
                } catch (Exception e) {
                    LOGGER.error("Couldnt change the file permissions ", e);

                }
                //载入到hbase表
                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
                loader.doBulkLoad(new Path(outputPathIndex), hTableIndex);
            } else {
                LOGGER.error("loading failed.");

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (hTableRws != null) {
                hTableRws.close();
            }
            if (hTableIndex != null) {
                hTableIndex.close();
            }
        }
        return 0;
    }

    public static void main(String[] args) {
        try {
            long startTime = System.currentTimeMillis();
            LOGGER.info("开始处理MR");
            //开始执行
            int exitCode = ToolRunner.run(new SaveHbaseBulkloadDriver(), args);
            long endTime = System.currentTimeMillis();
            LOGGER.info("处理结束： 用时: " + TimesUtil.formatTime(endTime - startTime));
            System.exit(exitCode);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }


}
