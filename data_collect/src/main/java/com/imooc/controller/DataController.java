package com.imooc.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.imooc.bean.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

/**
 * 数据接口V1.0
 * Created by xuwei
 */
@RestController//控制器类
@RequestMapping("/v1")//映射路径
public class DataController {
    private static final Logger logger = LoggerFactory.getLogger(DataController.class);
    /**
     * 测试接口
     * @param name
     * @return
     */
    @RequestMapping(value="/t1",method = RequestMethod.GET)
    public Status test(@RequestParam("name") String name) {
        Status status = new Status();
        System.out.println(name);
        return status;
    }

    /**
     * 接收用户行为数据接口
     * 将接受到的数据打平，解析data数组中的数据，拼接公共字段，一行转为多行
     * @param jsonObj
     * @return
     */
    @RequestMapping(value="/ua",method = RequestMethod.POST,consumes="application/json")
    public Status userAction(@RequestBody(required=false)JSONObject jsonObj) {
        Status status = new Status();

        //解析上报过来的用户行为数据

        //解析data中的数据
        JSONArray dataArr = jsonObj.getJSONArray("data");
        jsonObj.remove("data");
        for(int i=0;i<dataArr.size();i++){
            //解析每一条act数据
            JSONObject actObj = dataArr.getJSONObject(i);
            //将act相关的字段和公共字段拼接到一块
            JSONObject resObj = new JSONObject();
            resObj.putAll(jsonObj);
            resObj.putAll(actObj);
            //{"ver":"3.4.8","display":"1280x768","goods_id":"100086","osver":"7.1.1","platform":3,"uid":"1000030","act":2,"model":"huawei21","acttime":1593511199232,"location":6,"net":4,"xaid":"ab25617-c38910-m30","brand":"huawei","vercode":"35100044"}
            logger.info(resObj.toJSONString());
        }


        return status;
    }




}
