package com.lyf.bmall.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * 通过编写canal的客户端，从canalinstance的消息队列中获取mysql事实发生数据的变化
 * 将获取的数据发送给kafka
 * @date 17:44
 */
public class CanalApp {
    public static void main(String[] args) {

        //1.连接canal的服务器端
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("bw77", 11111), "example", ""
                , "");

        //2.抓取数据
        while (true){

            canalConnector.connect();

            //客户端订阅数据 告诉人家你要订阅哪个数据库的哪个表
            canalConnector.subscribe("bmall05a.*");

            //抓取数据  一个message  一次抓取有5个sql执行的结果集
            Message message = canalConnector.get(5);

            //如果该次没有抓取到数据  就休息一会  在抓取
            if (message.getEntries().size()==0){
                try {

                    Thread.sleep(5000);

                } catch (InterruptedException e) {

                    e.printStackTrace();

                }

                System.out.println("没有数据，休息5s钟。。。。");

            }else {

                //处理数据  5个sql执行完之后的结果集都在List<entry>
                for (CanalEntry.Entry entry:message.getEntries()){

                    //除去select这样的操作  还有一些操作也可能产生数据的变化
                    //我们只拿到有数据变化的那些操作影响的数据
                    if (entry.getEntryType()==CanalEntry.EntryType.ROWDATA){

                        //canal使用的序列化方式叫做protobuf技术
                        ByteString byteString = entry.getStoreValue();

                        CanalEntry.RowChange rowChange = null;

                        //使用反序列化工具进行反序列化
                        try {

                            rowChange = CanalEntry.RowChange.parseFrom(byteString);

                            List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();

                            //进行数据处理
                            CanalHandler canalHandler = new CanalHandler(entry.getHeader().getEventType(),entry.getHeader().getTableName(),rowDataList);

                            canalHandler.handle();

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                    }
                }
            }
        }
        //3.提取数据
        //4.将数据发送给kafka对应的topic

    }
}
