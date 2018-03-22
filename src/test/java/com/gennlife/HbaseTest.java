package com.gennlife;

import com.gennlife.util.HbaseUtils;
import org.junit.Test;

import java.io.IOException;

/**
 * @author liumingxin
 * @create 2018 16 9:41
 * @desc
 **/
public class HbaseTest {


    @Test
    public void HbaseTest() throws IOException {
        //        HbaseUtils.createTable("cesh111",new String[]{"patient_info","visit_info"});
        HbaseUtils.createTable("Hfile",new String[]{"data"});
//        HbaseUtils.insterRow("t2", "rw1", "cf1", "q1", "val1");
//        HbaseUtils.getData("testIndex", "pat_7455ea01cb4c5732133699e20eb01403", "data", "data");
//        HbaseUtils.getData("testPatient", "pat_7455ea01cb4c5732133699e20eb01403", "visit_info", "visit_info");
//        HbaseUtils.scanData("t2", "rw1", "rw2");
/*        HbaseUtils.deleRow("t2","rw1","cf1","q1");
        HbaseUtils.deleteTable("t2");*/
        System.out.println("get ok");
    }



}
