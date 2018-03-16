package com.gennlife;

import com.gennlife.util.HbaseUtils;

import java.io.IOException;

/**
 * @author liumingxin
 * @create 2018 16 9:41
 * @desc
 **/
public class HbaseTest {



    public static void main(String[] args) throws IOException {
//        createTable("testPatient",new String[]{"patient_info","visit_info"});
        HbaseUtils.createTable("testIndex",new String[]{"data"});
//        insterRow("t2", "rw1", "cf1", "q1", "val1");
//        getData("t2", "rw1", "cf1", "q1");
//        scanData("t2", "rw1", "rw2");
/*        deleRow("t2","rw1","cf1","q1");
        deleteTable("t2");*/
    }
}
