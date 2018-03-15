package com.gennlife.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author liumingxin
 * @create 2018 14 20:19
 * @desc  筛选组合数据
 **/
public class AnalysisJSON {

    private static final Logger LOGGER= LoggerFactory.getLogger(AnalysisJSON.class);

    private  HashMap<String,String> map = new HashMap<String, String>();

    public AnalysisJSON(){}

    public HashMap<String,String> getMap(String str){
        try{
            AnalysisString(str);
            return map;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 迭代解析字符串
     * @param str
     */
    public void AnalysisString(String str){
        try {
            Object o = JSON.parse(str);
            if(o instanceof JSONObject){
                  JSONObject jsonObject=JSONObject.parseObject(str);
                for(String key : jsonObject.keySet()){
                    String value = jsonObject.getString(key);
                    if("patient_info".equals(key)){
                        map.put(key,value);
                    }
                    if("PATIENT_SN".equals(key)){
                        map.put(key,value);
                    }
                    if("visits".equals(key)){
                        insertMap(value);
                    }
                    AnalysisString(value);
                }
            }else {
                JSONArray jsonArray = JSONArray.parseArray(str);
                int size = jsonArray==null ? 0:jsonArray.size();
                for (int i=0;i<size;i++){
                    AnalysisString(jsonArray.getString(i));
                }
            }
        }catch (Exception e){

        }
    }

    /**
     * 根据业务要求讲数据插入到map中
     * @param str
     */
    public void insertMap(String str){
        JSONArray jsonArray=JSONArray.parseArray(str);
        int size = jsonArray==null?0:jsonArray.size();
        for(int i=0;i<size;i++){
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            for(String key : jsonObject.keySet()){
                String value = jsonObject.getString(key);
                if(map.containsKey(key)){
                    String v1=map.get(key).substring(1,map.get(key).lastIndexOf("]"));
                    String v2=value.substring(1,value.lastIndexOf("]"));
                    map.put(key,"["+v1+","+v2+"]");
                }else {
                    map.put(key,value);
                }
            }
        }
    }

    public void clearMap(){
        map.clear();
    }


}
