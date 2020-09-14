package com.lyf.bmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.lyf.bmall.publisher.bean.Option;
import com.lyf.bmall.publisher.bean.Stat;
import com.lyf.bmall.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author shkstart
 * @date 19:00
 */
@Controller
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @GetMapping("/realtime-total")
    @ResponseBody
    public String realtimetotal(@RequestParam("date") String date){
        long total = publisherService.getDauTotal(date);
        List<Map> list = new ArrayList<Map>();
        Map dauMap = new HashMap<String,Object>();
        dauMap.put("id","dau");
        dauMap.put("name","当前日活");
        dauMap.put("value",total);
        list.add(dauMap);

        Double orderAmount = publisherService.getOrderAmount(date);
        Map orderAmountMap = new HashMap<String,Object>();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value",orderAmount);
        list.add(orderAmountMap);

        return JSONArray.toJSONString(list);
    }

    @GetMapping("/realtime-hour")
    @ResponseBody
    public String getRealTimeHours(@RequestParam("id") String id,@RequestParam("date") String date){

        if("dau".equals(id)) {
            Map<String, Long> dauTotalHours = publisherService.getDauTotalHours(date);
            String yesterday = getYesterday(date);
            Map<String, Long> dauYesterday = publisherService.getDauTotalHours(yesterday);
            Map json = new HashMap();
            json.put("today", dauTotalHours);
            json.put("yesterday", dauYesterday);
            return JSON.toJSONString(json);

        }else if ("order_amount".equals(id)){
            Map<String, Double> orderHourAmountTodayMap = publisherService.getOrderHourAmount(date);
            String yesterday = getYesterday(date);
            Map<String, Double> orderHourAmountYdayMap = publisherService.getOrderHourAmount(yesterday);
            Map json = new HashMap();
            json.put("today", orderHourAmountTodayMap);
            json.put("yesterday", orderHourAmountYdayMap);
            return JSON.toJSONString(json);
        }
        return null;
    }

    private String getYesterday(String today){

        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            Date date = sdf.parse(today);

            Date yesterday = DateUtils.addDays(date,-1);

            return sdf.format(yesterday);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }


    @GetMapping("/sale_detail")
    public String getSaleDeatilFromEs(@RequestParam("date") String date,@RequestParam("keyword") String keyword,@RequestParam("pageNo") int pageNo,@RequestParam("pageSize") int pageSize){
        Map<String, Object> saleDetailMap = publisherService.getSaleDetailFromEs(date, keyword, pageNo, pageSize);

        Map genderMap = (Map) saleDetailMap.get("genderMap");
        Map ageMap = (Map) saleDetailMap.get("ageMap");
        Long total = (Long) saleDetailMap.get("total");
        List<Map> saleList = (List<Map>) saleDetailMap.get("saleList");

        // 性别的饼图数据
        Long femaleCount = (Long) genderMap.get("F");
        Long maleCount = (Long) genderMap.get("M");

        double femaleRatio = Math.round(femaleCount*1000D/total)/10D;
        double maleRatio = Math.round(maleCount*1000D/total)/10D;

        List<Option> genderOptions = new ArrayList<>();
        genderOptions.add(new Option("男",maleRatio));
        genderOptions.add(new Option("女",femaleRatio));

        Stat genderStat = new Stat("性别占比", genderOptions);
        //年龄段饼图数据
        //把年龄：个数 调整为年龄段：占比
        Long age_20=0L;
        Long age20_30=0L;
        Long age30=0L;

        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry)o;
            String age = (String) entry.getKey();
            Long count = (Long) entry.getKey();
            Integer ageInt = Integer.valueOf(age);
            if (ageInt<20){
                age_20+=count;
            }else if (ageInt >= 20 && ageInt < 30){
                age20_30+=count;
            }else {
                age30+=count;
            }
        }

        Double age_20Ratio=0D;
        Double age20_30Ratio=0D;
        Double age30Ratio=0D;
        age_20Ratio = Math.round(age_20Ratio * 1000D / total) / 10D;
        age20_30Ratio = Math.round(age20_30Ratio * 1000D / total) / 10D;
        age30Ratio = Math.round(age30Ratio * 1000D / total) / 10D;

        List<Option> ageOptions = new ArrayList<>();
        ageOptions.add(new Option("20岁以下", age_20Ratio));
        ageOptions.add(new Option("20岁到30岁", age20_30Ratio));
        ageOptions.add(new Option("30岁以上", age30Ratio));
        Stat ageStat = new Stat("用户年龄占比", ageOptions);

        // 饼图列表
        List<Stat> StatList = new ArrayList<>();
        StatList.add(genderStat);
        StatList.add(ageStat);

        Map resultMap = new HashMap();

        resultMap.put("total", total);
        resultMap.put("stat", StatList);
        resultMap.put("detail", saleList);

        return JSON.toJSONString(resultMap);

    }

}
