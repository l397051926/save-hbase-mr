package com.gennlife.map;

import com.gennlife.handler.AnalysisJSON;
import com.gennlife.util.ConfigProperties;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liumingxin
 * @create 2018 14 10:36
 * @desc  rws mapper 到hbase
 **/
public class RWSHbaseMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put> {

    private String colFamily1 = null;
    private String colFamily2 = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        colFamily1 = ConfigProperties.RWS_COLFAMILY1;
        colFamily2 = ConfigProperties.RWS_COLFAMILY2;
//        super.setup(context);
    }

    @Override//map函数
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            //获取字符串
            String lineString = value.toString();
            AnalysisJSON analysisJSON = new AnalysisJSON();
            HashMap<String,String> map = analysisJSON.getMap(lineString.split("\u0001")[1]);

//            HashMap<String,String> map = analysisJSON.getMap(lineString.split("\t")[1]);

            String rowKey= map.get("PATIENT_SN");
            map.remove("PATIENT_SN");
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(colFamily1),Bytes.toBytes("patient_info"),Snappy.compress(map.get("patient_info")));
            map.remove("patient_info");
            for(Map.Entry<String,String> entry : map.entrySet()){
                put.addColumn(Bytes.toBytes(colFamily2),Bytes.toBytes(entry.getKey()),Snappy.compress(entry.getValue()));
            }

            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)),put);
//            context.getCounter(ImportFromFile.Counters.LINES).increment(1);
            analysisJSON.clearMap();
            analysisJSON=null;


        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
