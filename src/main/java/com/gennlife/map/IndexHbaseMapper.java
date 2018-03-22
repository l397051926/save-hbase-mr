package com.gennlife.map;

import com.gennlife.handler.AnalysisJSON;
import com.gennlife.util.ComPressUtils;
import com.gennlife.util.ConfigProperties;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author liumingxin
 * @create 2018 15 14:07
 * @desc 索引mapper 到hbase
 **/
public class IndexHbaseMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put> {

    private static final Logger LOGGER= LoggerFactory.getLogger(IndexHbaseMapper.class);
    private String colFamily1 = null;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        colFamily1 = ConfigProperties.INDEX_COLFAMILY1;
        super.setup(context);
    }

    @Override//map函数
    protected void map(LongWritable key, Text value, Context context) {

        String rowKey=null;
        String data = null;

        try {
            //获取字符串
            String lineString = value.toString();

             rowKey = lineString.split("\u0001")[0];
             data = lineString.split("\u0001")[1];

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(colFamily1),Bytes.toBytes("data"), ComPressUtils.compress(data));

            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)),put);

            LOGGER.info("正在处理 索引表:  rowKey: "+rowKey +"数据");
        }catch (Exception e){
            LOGGER.warn("PATIENT_SN:"+rowKey+"发生问题："+e.getMessage());
        }
    }
}