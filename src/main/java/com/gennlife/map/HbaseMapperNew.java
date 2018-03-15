package com.gennlife.map;

import com.gennlife.handler.AnalysisJSON;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author liumingxin
 * @create 2018 14 10:36
 * @desc  使用 hive连接方式
 **/
public class HbaseMapperNew extends Mapper<WritableComparable, HCatRecord,ImmutableBytesWritable,Put> {

    private String colFamily1 = null;
    private String colFamily2 = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        colFamily1 = "patient_info";
        colFamily2 = "visit_info";
//        super.setup(context);
    }


    @Override
    protected void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException {

        try {
            //获取字符串
            String data = (String)value.get(1);
            AnalysisJSON analysisJSON = new AnalysisJSON();
            HashMap<String,String> map = analysisJSON.getMap(data);
            analysisJSON.clearMap();
            String rowKey= map.get("PATIENT_SN");
            map.remove("PATIENT_SN");
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(colFamily1),Bytes.toBytes("patient_info"),Snappy.compress(map.get("patient_info")));
            map.remove("patient_info");
            for(Map.Entry<String,String> entry : map.entrySet()){
                put.addColumn(Bytes.toBytes(colFamily2),Bytes.toBytes(entry.getKey()),Snappy.compress(entry.getValue()));
            }

            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)),put);
            analysisJSON =null;
//            context.getCounter(ImportFromFile.Counters.LINES).increment(1);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
