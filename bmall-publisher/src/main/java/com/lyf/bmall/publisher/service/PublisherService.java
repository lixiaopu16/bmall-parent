package com.lyf.bmall.publisher.service;


import java.util.List;
import java.util.Map;

/**
 * @author shkstart
 * @date 18:57
 */

public interface PublisherService {
    public Long getDauTotal(String date);

    public Map getDauTotalHours(String date);

    public Double getOrderAmount(String date);

    public Map<String,Double> getOrderHourAmount(String date);

    public Map<String,Object> getSaleDetailFromEs(String date,String keyword,int pageNo,int pageSize);

    }
