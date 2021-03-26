package com.danier.flink.study.model.config;

import lombok.Data;

import java.util.Properties;

/**
 * @Date 2021/3/26 6:00 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc:
 */
@Data
public class MysqlConfigVo {

    private String name;
    private volatile String url;
    private volatile String username;
    private volatile String password;
    private volatile String driverClassName = "com.mysql.jdbc.Driver";

    private volatile int maxActive = 8;
    private volatile int minIdle = 0;
    private volatile int initialSize = 0;
    private volatile String validationQuery;
    private volatile boolean testOnBorrow;
    private volatile boolean testOnReturn;
    private volatile boolean testWhileIdle;
    private volatile long timeBetweenEvictionRunsMillis = 60000L;
    private volatile long minEvictableIdleTimeMillis = 1800000L;

    public MysqlConfigVo(Properties defaultProperties) {
        this.name = defaultProperties.getProperty("mysql.name");
        this.url = defaultProperties.getProperty("mysql.url");
        this.username = defaultProperties.getProperty("mysql.username");
        this.password = defaultProperties.getProperty("mysql.password");
        this.driverClassName = defaultProperties.getProperty("mysql.driverClassName");
        this.maxActive = Integer.valueOf(defaultProperties.getProperty("mysql.maxActive", "8"));
        this.minIdle = Integer.valueOf(defaultProperties.getProperty("mysql.minIdle", "0"));
        this.initialSize = Integer.valueOf(defaultProperties.getProperty("mysql.initialSize", "1"));
        this.validationQuery = defaultProperties.getProperty("mysql.validationQuery");
        this.testOnBorrow = Boolean.valueOf(defaultProperties.getProperty("mysql.testOnBorrow"));
        this.testOnReturn = Boolean.valueOf(defaultProperties.getProperty("mysql.testOnReturn"));
        this.testWhileIdle = Boolean.valueOf(defaultProperties.getProperty("mysql.testWhileIdle"));
        this.timeBetweenEvictionRunsMillis = Long.valueOf(defaultProperties.getProperty("mysql.timeBetweenEvictionRunsMillis", "60000"));
        this.minEvictableIdleTimeMillis = Long.valueOf(defaultProperties.getProperty("mysql.minEvictableIdleTimeMillis", "1800000"));
    }
}
