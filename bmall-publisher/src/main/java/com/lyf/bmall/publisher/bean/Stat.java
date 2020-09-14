package com.lyf.bmall.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author shkstart
 * @date 8:32
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Stat {
    String title;

    List<Option> options;
}
