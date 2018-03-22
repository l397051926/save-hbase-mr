package com.gennlife;

import com.gennlife.util.ConfigProperties;
import com.gennlife.util.TimesUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
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

    @Test
    public void TestDemo1(){
        //        Map<String,String> map  = new HashMap<String,String>();
//        map.put("a","b");
//        String a=map.get("C");
//        System.out.println(a);
//       for (int i=0;i<args.length;i++){
//           System.out.println(args[i]);
//       }

//        long d=8123400;
//        System.out.println( TimesUtil.formatTime(d));
//        System.out.println(60*60*24*1000);

        LOG.trace("this is trace");
        LOG.debug("this is debug");
        LOG.info("this is info");
        LOG.error("this is error");
        LOG.warn("this is warn");
        LOG.error("this is error");
        System.out.println(ConfigProperties.HBASE_ZOOKEEPER_QUORUM);
    }


}
