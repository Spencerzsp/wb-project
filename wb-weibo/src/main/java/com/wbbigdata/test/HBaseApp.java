package com.wbbigdata.test;

import com.wbbigdata.constants.Constants;
import com.wbbigdata.dao.HBaseDao;
import com.wbbigdata.utils.HBaseUtil;

import java.io.IOException;

public class HBaseApp {

    public static void init(){


        try {
            //创建命名空间
//            HBaseUtil.createNameSpace(Constants.NAMESPACE);

            //创建微博内容表
            HBaseUtil.createTable(Constants.CONTENT_TABLE, Constants.CONTENT_TABLE_VERSIONS, Constants.CONTENT_TABLE_CF);

            //创建用户关系表
            HBaseUtil.createTable(Constants.RELATION_TABLE,
                    Constants.RELATION_TABLE_VERSIONS,
                    Constants.RELATION_TABLE_CF1, Constants.RELATION_TABLE_CF2);

            //创建收件箱表
            HBaseUtil.createTable(Constants.INBOX_TABLE, Constants.INBOX_TABLE_VERSIONS, Constants.INBOX_TABLE_CF);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {

        //初始化
//        init();

        //1001发布微博
//        HBaseDao.publishWeibo("1001", "我要升职加薪迎娶白富美！！！");

        //1002关注1001和1003
//        HBaseDao.addAttends("1001", "1003");

        //获取1002初始化页面
//        HBaseDao.init("1002");
//
        System.out.println("*********111*********");

        HBaseDao.getWeibo("1001");


    }
}
