package com.lyf.bmall.publisher.mapper;

import com.lyf.bmall.publisher.bean.OrderHourAmount;

import java.util.List;

/**
 * @author shkstart
 * @date 17:18
 */
public interface OrderMapper {

    public Double getOrderAmount(String date);

    public List<OrderHourAmount> getOrderHourAmount(String date);


}
