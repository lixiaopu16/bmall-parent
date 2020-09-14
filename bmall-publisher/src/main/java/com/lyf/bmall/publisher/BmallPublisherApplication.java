package com.lyf.bmall.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.lyf.bmall.publisher.mapper")
public class BmallPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(BmallPublisherApplication.class, args);
    }

}
