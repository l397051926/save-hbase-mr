package com.gennlife;

import com.gennlife.util.TimesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author liumingxin
 * @create 2018 16 9:33
 * @desc
 **/
public class TestDemo {
    private static final Logger LOG= LoggerFactory.getLogger(TestDemo.class);
    public static void main(String[] args) {
//        Map<String,String> map  = new HashMap<String,String>();
//        map.put("a","b");
//        String a=map.get("C");
//        System.out.println(a);
//       for (int i=0;i<args.length;i++){
//           System.out.println(args[i]);
//       }

        long x=126935;
        System.out.println(TimesUtil.formatTime(x));


        System.out.println();
    }


}
