package com.gennlife;

import com.gennlife.util.HbaseUtils;

import java.io.IOException;

/**
 * @author liumingxin
 * @create 2018 16 9:41
 * @desc
 **/
public class HbaseTest {



    public static void mai1n(String[] args) throws IOException {
//        HbaseUtils.createTable("testPatient",new String[]{"patient_info","visit_info"});
//        HbaseUtils.createTable("testIndex",new String[]{"data"});
//        HbaseUtils.insterRow("t2", "rw1", "cf1", "q1", "val1");
        HbaseUtils.getData("testIndex", "pat_7455ea01cb4c5732133699e20eb01403", "data", "data");
//        HbaseUtils.scanData("t2", "rw1", "rw2");
/*        HbaseUtils.deleRow("t2","rw1","cf1","q1");
        HbaseUtils.deleteTable("t2");*/
        System.out.println("get ok");
    }
}
