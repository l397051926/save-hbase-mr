package com.gennlife;

import com.alibaba.fastjson.JSON;
import com.gennlife.handler.AnalysisJSON;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.Map;

/**
 * @author liumingxin
 * @create 2018 16 10:26
// * @desc
// **/
//public class DataverifyTest {
//
////    @Test
//    public void getDatasource() throws Exception {
//        String inputPart="data.json"; String tableName="";
//        BufferedReader br;
//        br = new BufferedReader(new InputStreamReader(new FileInputStream(inputPart), "utf-8"));
//        String onLine = null;
//        StringBuffer str = new StringBuffer();
//        while ((onLine = br.readLine()) != null) {
//            str.append(onLine.trim());
//        }
//        String ss = str.toString();
//        Map<String,String> map = new AnalysisJSON().getMap(ss);
//        System.out.println(ss);
//
//
//
//
////        String x = "{\"a\":\"b\"}";
//        Object o = JSON.parse(x);
//        System.out.println("A");
//        BufferedReader br;
////        br = new BufferedReader(new InputStreamReader(new FileInputStream("dta.txt"),"utf-8"));
//        String onLine = null;
//        StringBuffer str= new StringBuffer();
//        while((onLine = br.readLine())!=null){
//            str.append(onLine);
//        }
//        String ss=str.toString();
////        byte[] oo=Snappy.compress(ss);
////        String xx=new String(oo);
////        byte[] gg = Snappy.uncompress(oo);
////        String zz=new String(gg);
////
////        System.out.println(ss);
//    }
//
//
//}
