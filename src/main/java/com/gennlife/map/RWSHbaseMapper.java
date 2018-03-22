package com.gennlife.map;

import com.gennlife.handler.AnalysisJSON;
import com.gennlife.util.ComPressUtils;
import com.gennlife.util.ConfigProperties;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger LOGGER= LoggerFactory.getLogger(RWSHbaseMapper.class);
    private String colFamily1 = null;
    private String colFamily2 = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        colFamily1 = ConfigProperties.RWS_COLFAMILY1;
        colFamily2 = ConfigProperties.RWS_COLFAMILY2;
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)  {

        String rowKey=null;
        try {
            //获取字符串
            String lineString = value.toString();
            AnalysisJSON analysisJSON = new AnalysisJSON();
            HashMap<String,String> map = analysisJSON.getMap(lineString.split("\u0001")[1]);

            rowKey= map.get("PATIENT_SN");
            map.remove("PATIENT_SN");

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(colFamily1),Bytes.toBytes("patient_info"), ComPressUtils.compress(map.get("patient_info")));

            map.remove("patient_info");
            for(Map.Entry<String,String> entry : map.entrySet()){

                put.addColumn(Bytes.toBytes(colFamily2),Bytes.toBytes(entry.getKey()),ComPressUtils.compress(entry.getValue()));
            }

            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)),put);

            analysisJSON.clearMap();

            LOGGER.info("正在处理RWS表:  rowKey: "+rowKey +"数据");

        }catch (Exception e){
            LOGGER.error("PATIENT_SN:"+rowKey+"发生问题："+e.getMessage());
        }
    }


}
