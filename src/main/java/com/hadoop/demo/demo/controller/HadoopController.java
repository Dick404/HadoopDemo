package com.hadoop.demo.demo.controller;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.hadoop.demo.demo.util.HadoopUtil;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOError;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

@RestController
@RequestMapping("/hadoop")
public class HadoopController {

    @PostMapping("/hdfs/mkdir")
    public String mkdir(@RequestParam("path") String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return "请求参数为空";
        }
        // 文件对象
        FileSystem fs = HadoopUtil.getFileSystem();
        // 目标路径
        Path newPath = new Path(path);
        // 创建空文件夹
        boolean isOk = fs.mkdirs(newPath);
        fs.close();
        if (isOk) {
            return "create dir success";
        } else {
            return "create dir fail";
        }
    }

    @PostMapping("/hdfs/createFile")
    public String createFile(@RequestParam("path") String path, @RequestParam("file") MultipartFile file) throws Exception {
        if (StringUtils.isEmpty(path) || null == file.getBytes()) {
            return "请求参数为空";
        }
        String fileName = file.getOriginalFilename();
        FileSystem fs = HadoopUtil.getFileSystem();
        // 上传时默认当前目录，后面自动拼接文件的目录
        Path newPath = new Path(path + "/" + fileName);
        // 打开一个输出流
        FSDataOutputStream outputStream = fs.create(newPath);
        outputStream.write(file.getBytes());
        outputStream.close();
        fs.close();
        return "create file success";
    }

    @PostMapping("/hbase/adddata")
    public String AddDataToHbase(@RequestParam("rowkey") String rowKey, @RequestParam("tableName") String tableName, @RequestParam("column1") String [] column1, @RequestParam("value1") String [] value1) throws IOException{
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        Configuration conf = HadoopUtil.getHbaseConfiguration();
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//
        // 获取表
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                .getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i++) {
            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
            if (familyName.equals("ID")) { // article列族put数据
                for (int j = 0; j < column1.length; j++) {
                    put.add(Bytes.toBytes(familyName), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
        }
        table.put(put);
        return "add successful!";
    }

    @PostMapping("/hbase/create")
    public String CreateTable(@RequestParam("tableame") String tableName) throws IOException{
        Configuration conf = HadoopUtil.getHbaseConfiguration();
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        String [] family = new String [5];
        family[0] = "studentinfo";
        for (int i = 0; i < family.length; i++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(tableName)) {
            System.out.println("table Exists!");
            return "create failed!";
        } else {
            admin.createTable(desc);
        }
        return "create successful!";
    }
}


