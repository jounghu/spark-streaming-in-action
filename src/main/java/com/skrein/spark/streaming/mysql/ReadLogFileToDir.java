package com.skrein.spark.streaming.mysql;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * @author :hujiansong
 * @date :2019/10/22 16:44
 * @since :1.8
 */
public class ReadLogFileToDir {
    public static void main(String[] args) throws IOException {
        String file = "C:\\ad-dev\\spark-streaming-in-action\\src\\main\\resources\\logapp.log";
        FileInputStream is = new FileInputStream(file);
        FileOutputStream os = new FileOutputStream(new File("C:\\ad-dev\\spark-streaming-in-action\\spark-streaming-dir\\" + UUID.randomUUID()));
        IOUtils.copy(is, os);
    }
}
