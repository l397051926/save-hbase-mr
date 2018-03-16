package com.gennlife.util;

import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * @author liumingxin
 * @create 2018 16 12:01
 * @desc  压缩工具类
 **/
public class ComPressUtils {

    /**
     * 压缩方式 默认snappy
     * @param str
     * @return
     */
    public static byte[] compress(String str) throws IOException {
        if("gzip".equals(ConfigProperties.COMPRESS_NAME)){
            return GZIPUtils.compress(str);
        }else{
            return Snappy.compress(str);
        }

    }

    /**
     * 解压缩方式 默认snappy
     * @param input
     * @return
     */
    public static byte[] unCompress(byte[] input) throws IOException {
        if("gzip".equals(ConfigProperties.COMPRESS_NAME)){
            return GZIPUtils.uncompress(input);
        }else{
            return Snappy.uncompress(input);
        }
    }
}
