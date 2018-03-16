package com.gennlife;

import org.apache.hadoop.hbase.util.Bytes;
import org.xerial.snappy.Snappy;

import java.io.*;

/**
 * @author liumingxin
 * @create 2018 16 10:26
 * @desc
 **/
public class Dataverify {
    public static void main(String[] args) throws IOException {
        BufferedReader br;
        br = new BufferedReader(new InputStreamReader(new FileInputStream("data.txt"),"utf-8"));
        String onLine = null;
        StringBuffer str= new StringBuffer();
        while((onLine = br.readLine())!=null){
            str.append(onLine);
        }
        String ss=str.toString();
        byte[] oo=Snappy.compress(ss);
        String xx=new String(oo);
        byte[] gg = Snappy.uncompress(oo);
        String zz=new String(gg);

        System.out.println(ss);
    }
}