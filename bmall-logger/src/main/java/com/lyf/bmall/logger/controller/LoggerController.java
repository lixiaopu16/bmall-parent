package com.lyf.bmall.logger.controller;

import com.alibaba.fastjson.JSONObject;
import com.lyf.bmall.common.constant.BmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author shkstart
 * @date 17:23
 */
@Controller
@Slf4j  //有了这个注释  就可以用一个不用声明的对象log
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
    @ResponseBody
    public String log(@RequestParam("logString")String logs){

        //log.info(logs);

        //有可能在前端程序员没有时间戳字段  在后端收集到kafka之前 我们可以给他添加一个
        JSONObject jsonObjecct = JSONObject.parseObject(logs);
        jsonObjecct.put("ts",System.currentTimeMillis());

        //数据分为两种不同格式
        if ("startup".equals(jsonObjecct.get("type"))){
            kafkaTemplate.send(BmallConstants.KAFKA_STARTUP,jsonObjecct.toString());
        }else {
            kafkaTemplate.send(BmallConstants.KAFKA_EVENT,jsonObjecct.toString());
        }

        return "success";
    }
}
