package com.lyf.bmall.publisher.mapper;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Map;

/**
 * @author shkstart
 * @date 18:38
 */
@Mapper
public interface DauMapper {

    /**
     * 查询截止到当前时间的uv的数量
     */
    public Long selectDauTotal(String date);
    public List<Map> getDauTotalHours(String date);

}
