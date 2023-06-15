package com.imooc.useraction;

import com.alibaba.fastjson.JSONObject;
import com.imooc.useraction.utils.HttpUtil;
import com.imooc.useraction.utils.MyDbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;


/**
 * 需求：生成商品订单相关数据(服务端数据)，初始化到MySql中
 *
 * 【注意】：在执行代码之前需要先执行init_mysql_tables.sql脚本进行数据库和表的初始化
 * Created by xuwei
 */
public class GenerateGoodsOrderData {
    private final static Logger logger = LoggerFactory.getLogger(GenerateGoodsOrderData.class);

    public static void main(String[] args) {
        //通过接口获取用户行为数据
        String dataUrl = "http://data.xuwei.tech/d1/go1";
        JSONObject paramObj = new JSONObject();
        //TODO code:校验码，需要到微信公众号上获取有效校验码,具体操作流程见电子书
        paramObj.put("code","imooc");//校验码
        paramObj.put("date","2026-01-01");//指定数据产生的日期
        paramObj.put("user_num",100);//指定生成的用户数量
        paramObj.put("order_num",1000);//指定生成的订单数量
        //insert into t1(...) values(...)
        JSONObject dataObj = HttpUtil.doPost(dataUrl, paramObj);

        //判断获取的用户行为数据是否正确
        boolean flag = dataObj.containsKey("error");
        if(!flag){
            //通过JDBC的方式初始化数据到MySQL中
            String data = dataObj.getString("data");
            String[] splits = data.split("\n");
            long start = System.currentTimeMillis();
            logger.info("===============start init===============");
            ArrayList<String> tmpSqlList = new ArrayList<String>();
            for(int i=0;i<splits.length;i++){
                tmpSqlList.add(splits[i]);
                if(tmpSqlList.size()%100==0){
                    MyDbUtils.batchUpdate(tmpSqlList);
                    tmpSqlList.clear();
                }
            }
            //把剩余的数据批量添加到数据库中
            MyDbUtils.batchUpdate(tmpSqlList);
            logger.info("===============end init==============");
            long end = System.currentTimeMillis();
            logger.info("===============耗时: "+(end-start)/1000+"秒===============");
        }else{
            logger.error("获取商品订单相关数据错误："+dataObj.toJSONString());
        }
    }
}
