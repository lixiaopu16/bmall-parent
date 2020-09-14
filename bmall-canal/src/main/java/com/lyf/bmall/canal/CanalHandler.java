package com.lyf.bmall.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.lyf.bmall.canal.utils.MyKafkaSender;
import com.lyf.bmall.common.constant.BmallConstants;

import java.util.List;

/**
 * @author shkstart
 * @date 18:20
 */
public class CanalHandler {
    private CanalEntry.EventType eventType;
    private String tableName;
    private List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handle(){

        if (tableName.equals("order_info") && eventType.equals(CanalEntry.EventType.INSERT)){

            rowDataList2kafka(BmallConstants.KAFKA_ORDER_INFO);


        }else if(tableName.equals("user_info")&& eventType.equals(CanalEntry.EventType.INSERT)||eventType.equals(CanalEntry.EventType.UPDATE)){

            rowDataList2kafka(BmallConstants.KAFKA_USER_INFO);

        }else if(tableName.equals("order_detail")&& eventType.equals(CanalEntry.EventType.INSERT)){
            rowDataList2kafka(BmallConstants.KAFKA_ORDER_DETAIL);
        }

    }

    //将rowDataList中的数据 发送给 指定kafka的topic
    private void rowDataList2kafka(String topic){
        for (CanalEntry.RowData rowData:rowDataList){
            //如果是insert update after操作     delete before操作
            List<CanalEntry.Column> columnList = rowData.getAfterColumnsList();
            JSONObject json = new JSONObject();
            for (CanalEntry.Column column:columnList){
                String columnName = column.getName();
                String columnValue = column.getValue();
                json.put(columnName,columnValue);
            }

            //之后再将json发送给kafka
            MyKafkaSender.send(topic,JSONObject.toJSONString(json));

        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
