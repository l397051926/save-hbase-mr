package com.gennlife;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liumingxin
 * @create 2018 16 9:33
 * @desc
 **/
public class TestDemo {
    public static void main(String[] args) {
        Map<String,String> map  = new HashMap<String,String>();
        map.put("a","b");
        String a=map.get("C");
        System.out.println(a);
    }
}
