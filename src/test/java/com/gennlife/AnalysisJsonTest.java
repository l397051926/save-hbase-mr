//package com.gennlife;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.gennlife.handler.AnalysisJSON;
//import com.google.gson.JsonObject;
//
//import java.io.*;
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * @author liumingxin
// * @create 2018 14 18:06
// * @desc
// **/
//public class AnalysisJsonTest {
//
//    private static Map<String,String> map = new HashMap<String, String>();
//
//    public static void main(String[] args) throws IOException {
//        System.out.println("start-*---");
//
//        BufferedReader br;
//        br = new BufferedReader(new InputStreamReader(new FileInputStream("o.txt"),"utf-8"));
//        String onLine = null;
//        StringBuffer str= new StringBuffer();
//        while((onLine = br.readLine())!=null){
//            str.append(onLine);
//        }
//        String ss=str.toString();
////        get(ss);
////        System.out.println("gg");
//
//        AnalysisJSON analysisJSON = new AnalysisJSON();
//        Map<String,String> map = analysisJSON.getMap(ss);
//
//        System.out.println("gg");
//
//
//    }
//
//    public static void get(String str){
//
//        try {
//            Object o = JSON.parse(str);
//            if(o instanceof JSONObject){
//                System.out.println("true");
//
//                JSONObject jsonObject=JSONObject.parseObject(str);
//                for(String key : jsonObject.keySet()){
//                    String value = jsonObject.getString(key);
//                    if("patient_info".equals(key)){
//                        map.put(key,value);
//                    }
//                    if("PATIENT_SN".equals(key)){
//                        map.put(key,value);
//                    }
//                    if("visits".equals(key)){
//                        putMap(value);
//                    }
//                    get(value);
//                }
//            }else {
//                System.out.println("false");
//
//                JSONArray jsonArray = JSONArray.parseArray(str);
//                int size = jsonArray==null ? 0:jsonArray.size();
//                for (int i=0;i<size;i++){
//                    get(jsonArray.getString(i));
//                }
//            }
//        }catch (Exception e){
//            System.out.println("is not json");
//        }
//    }
//
//    public static void putMap(String json){
//        JSONArray jsonArray=JSONArray.parseArray(json);
//        int size = jsonArray==null?0:jsonArray.size();
//        for(int i=0;i<size;i++){
//            JSONObject jsonObject = jsonArray.getJSONObject(i);
//            for(String key : jsonObject.keySet()){
//                String value = jsonObject.getString(key);
//                if(map.containsKey(key)){
//                    String v1=map.get(key).substring(1,map.get(key).lastIndexOf("]"));
//                    String v2=value.substring(1,value.lastIndexOf("]"));
//                    map.put(key,"["+v1+","+v2+"]");
//                }else {
//                    map.put(key,value);
//                }
//            }
//        }
//    }
//
//}
