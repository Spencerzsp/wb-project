package com.wbbigdata.dao;

import com.wbbigdata.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 1.发布微博
 * 2.删除微博
 * 3.关注用户
 * 4.取关用户
 * 5.获取用户微博详情
 * 6.获取用户的初始化页面
 */
public class HBaseDao {

    // 1.发布微博
    public static void publishWeibo(String uid, String content) throws IOException {

        //获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //第一部分：操作微博内容表
        // 1.获取微博内容表对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        // 2.获取当前时间戳
        long ts = System.currentTimeMillis();

        // 3.获取RowKey
        String rowKey = uid + "_" + ts;

        // 4.创建Put对象
        Put contPut = new Put(Bytes.toBytes(rowKey));

        // 5.给Put对象赋值
        contPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes("content"), Bytes.toBytes(content));

        // 6.插入数据操作
        contTable.put(contPut);


        //第二部分：操作微博发布收件箱表(即用户发布微博后，关注他的粉丝可以看到)
        // 1.获取用户关系表对象
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        // 2.获取当前发布微博人的fans列族信息
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
        Result result = relaTable.get(get);

        // 3.创建一个集合，用于存放微博内容表的Put对象
        ArrayList<Put> inboxPuts = new ArrayList<Put>();

        // 4.遍历粉丝
        for (Cell cell : result.rawCells()) {

            // 5.构建微博收件箱表的Put对象
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));

            // 6.给收件箱表的Put对象赋值
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF),Bytes.toBytes(uid), Bytes.toBytes(rowKey));

            inboxPuts.add(inboxPut);
        }

        //判断是否有粉丝
        if (inboxPuts.size() > 0){

            // 获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

            // 执行收件箱表数据插入操作
            inboxTable.put(inboxPuts);

            //关闭收件箱表
            inboxTable.close();
        }

        //关闭资源
        relaTable.close();
        contTable.close();
        connection.close();
    }

    // 2.关注用户
    public static void addAttends(String uid, String... attends) throws IOException {

        // 校验是否添加了待关注的人
        if(attends.length <= 0) {
            System.out.println("请选择待关注的人！！！");
            return;
        }

        // 获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //第一部分：操作用户关系表
        // 1.获取用户关系表对象
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        // 2.创建一个集合，用于存放用户关系表的Put对象
        ArrayList<Put> relaPuts = new ArrayList<Put>();


        // 3.创建操作者的Put对象
        Put uidPut = new Put(Bytes.toBytes(uid));

        // 4.循环创建被关注者的Put对象
        for (String attend : attends) {

            // 5.给操作者的Put对象赋值
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(attend), Bytes.toBytes(attend));

            // 6.创建被关注者的Put对象
            Put attendPut = new Put(Bytes.toBytes(attend));

            // 7.给被关注者的Put对象赋值
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));

            // 8.将被关注者的Put对象放入集合
            relaPuts.add(attendPut);
        }

        // 9.将操作者的Put对象添加至集合
        relaPuts.add(uidPut);

        // 10.执行用户关系表的插入数据操作
        relaTable.put(relaPuts);


        //第二部分：操作收件箱表
        // 1.获取微博内容对象表
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        // 2.创建收件箱表的Put对象
        Put inboxPut = new Put(Bytes.toBytes(uid));

        // 3.循环attends，获取每个被关注者的近期发布的微博
        for (String attend : attends) {
            // 4.获取当前被关注者的近期发布的微博(scan) ->集合ResultScanner
            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));

            ResultScanner resultScanner = contTable.getScanner(scan);

            long ts = System.currentTimeMillis();
            // 5.对获取的值进行遍历
            for (Result result : resultScanner) {

                // 6.给收件箱表的Put对象赋值
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend), ts++, result.getRow());



            }

        }

        // 7.判断当前的Put对象是否为空
        if(true){

            // 获取收件箱表
        }

        // 关闭资源
    }


    // 获取初始化页面数据
    public static void init(String uid) throws IOException {

        //获取Connection连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //获取微博内容表对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        //创建收件箱表Get对象，并获取数据(设置最大版本)
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);

        //遍历获取到的数据
        for (Cell cell : result.rawCells()) {

            //构建微博内容表Get对象
            Get contGet = new Get(CellUtil.cloneValue(cell));
            Result contResult = contTable.get(contGet);

            //解析内容并打印
            for (Cell contCell : contResult.rawCells()) {
                System.out.println("RK: " + Bytes.toString(CellUtil.cloneRow(contCell)) +
                        ", CF: " + Bytes.toString(CellUtil.cloneFamily(contCell)) +
                        ", CN: " + Bytes.toString(CellUtil.cloneQualifier(contCell)) +
                        ", Value: " + Bytes.toString(CellUtil.cloneValue(contCell))
                );
            }
        }

        //关闭资源
        inboxTable.close();
        contTable.close();
        connection.close();

    }

    //获取某个人的所有微博详情
    public static void getWeibo(String uid) throws IOException {

        //获取Connction连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //获取微博内容表对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //构建scan对象
        Scan scan = new Scan();

        //构建过滤器
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));
        scan.setFilter(rowFilter);

        //获取数据
        ResultScanner resultScanner = contTable.getScanner(scan);

        //解析数据并打印
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RK: " + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ", CF: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ", CN: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ", Value: " + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }

        //关闭资源
        contTable.close();
    }

}
