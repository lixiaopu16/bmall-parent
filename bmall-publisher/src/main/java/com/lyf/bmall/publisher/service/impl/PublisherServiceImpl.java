package com.lyf.bmall.publisher.service.impl;

import com.lyf.bmall.publisher.bean.OrderHourAmount;
import com.lyf.bmall.publisher.mapper.DauMapper;
import com.lyf.bmall.publisher.mapper.OrderMapper;
import com.lyf.bmall.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author shkstart
 * @date 18:58
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map<String,Long> getDauTotalHours(String date) {

        Map dataHours = new HashMap();

        List<Map> dauList = dauMapper.getDauTotalHours(date);

        for (int i = 0;i<dauList.size();i++){
            Map map = dauList.get(i);

            dataHours.put(map.get("HOURS"),map.get("CNTS"));
        }

        return dataHours;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.getOrderAmount(date);
    }

    @Override
    public Map<String, Double> getOrderHourAmount(String date) {

        //把List集合转换成map
        HashMap<String, Double> hourAmountMap = new HashMap<>();
        List<OrderHourAmount> orderHourAmountList = orderMapper.getOrderHourAmount(date);
        for (OrderHourAmount orderHourAmount : orderHourAmountList) {
            hourAmountMap.put(orderHourAmount.getCreateHour(),orderHourAmount.getSumOrderAmount());
        }

        return hourAmountMap;
    }

    //存入es  未完成
    public Map<String,Object> getSaleDetailFromEs(String date,String keyword,int pageNo,int pageSize){


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //构造过滤，匹配条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(genderAggs);
        searchSourceBuilder.aggregation(ageAggs);

        //分页
        searchSourceBuilder.from((pageNo-1)*pageSize);
        searchSourceBuilder.size(pageSize);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).build();

        HashMap<String, Object> resultMap = new HashMap<>();
        try {
            SearchResult searchResult = jestClient.execute(search);

            resultMap.put("total",searchResult.getTotal());//总数

            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);

            List<Map> saleList = new ArrayList<>();
            for (SearchResult.Hit<Map,Void> hit:hits){
                saleList.add(hit.source);
            }
            resultMap.put("saleList",saleList);//明细

            Map genderMap = new HashMap<>();
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_gender").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                genderMap.put(bucket.getKey(),bucket.getCount());
            }
            resultMap.put("genderMap",genderMap);//性别聚合

            Map ageMap = new HashMap<>();
            List<TermsAggregation.Entry> agebuckets = searchResult.getAggregations().getTermsAggregation("groupby_age").getBuckets();
            for (TermsAggregation.Entry bucket : agebuckets) {
                genderMap.put(bucket.getKey(),bucket.getCount());
            }
            resultMap.put("ageMap",ageMap);//年龄聚合

        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultMap;
    }

}
