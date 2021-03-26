package com.danier.flink.study.stream.source;

import com.alibaba.druid.pool.DruidDataSource;
import com.danier.flink.study.model.RuleVo;
import com.danier.flink.study.model.enums.AggregateTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * @Date 2021/3/26 5:37 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc: 风控规则source 按照指定duration间隔更新周期进行更新
 */
@Slf4j
public class RuleSource extends RichSourceFunction<Map<Integer, RuleVo>> {

    @Override
    public void run(SourceContext<Map<Integer, RuleVo>> sourceContext) throws Exception {
        while (isRunning) {
            if (!isTestRule) {
                // fixme 实现this.open方法。
                throw new RuntimeException("##### 在未使用测试规则情况下，需要自己实现 RuleSource.open方法，详情见RuleSource代码.");
            } else {
                Map<Integer, RuleVo> testRules = this.getTestRules();
                //输出规则流
                sourceContext.collect(testRules);
            }
            //线程睡眠
            Thread.sleep(duration);
        }
    }

    @Override
    public void cancel() {
        this.free();
    }

    @Override
    public void close() {
        this.free();
    }

    private void free() {
        this.isRunning = false;
        this.ruleJdbcTemplate = null;
        if (this.ruleSource != null) {
            this.ruleSource.close();
            this.ruleSource = null;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // FIXME 加载外部mysql数据源，需要新增MysqlConfigVo配置。
//        MysqlConfigVo config = new MysqlConfigVo();
//        this.ruleSource = new DruidDataSource();
//        ruleSource.setName(config.getName());
//        ruleSource.setDriverClassName(config.getDriverClassName());
//        ruleSource.setUrl(config.getUrl());
//        ruleSource.setUsername(config.getUsername());
//        ruleSource.setPassword(config.getPassword());
//        ruleSource.setMaxActive(config.getMaxActive());
//        ruleSource.setMinIdle(config.getMinIdle());
//        ruleSource.setInitialSize(config.getInitialSize());
//        ruleSource.setValidationQuery(config.getValidationQuery());
//        ruleSource.setTestOnBorrow(config.isTestOnBorrow());
//        ruleSource.setTestOnReturn(config.isTestOnReturn());
//        ruleSource.setTestWhileIdle(config.isTestWhileIdle());
//        ruleSource.setTimeBetweenEvictionRunsMillis(config.getTimeBetweenEvictionRunsMillis());
//        ruleSource.setMinEvictableIdleTimeMillis(config.getMinEvictableIdleTimeMillis());
//        this.ruleSource.init();
//        this.ruleJdbcTemplate = new JdbcTemplate(this.ruleSource);
//        log.info("##### open and init rule mysql connection poll {}.....", config);
    }

    public Map<Integer, RuleVo> getTestRules() {
        RuleVo countR = new RuleVo(1, AggregateTypeEnum.COUNT);
        RuleVo distinctR = new RuleVo(2, AggregateTypeEnum.DISTINCT);
        Map<Integer, RuleVo> r = new HashMap<>(2);
        r.put(countR.getRuleId(), countR);
        r.put(distinctR.getRuleId(), distinctR);
        return r;
    }

    public RuleSource(boolean isTestRule, Long duration) {
        this.isTestRule = isTestRule;
        this.duration = duration;

        // FIXME 规则更新sql
        this.ruleSql = "xxxxx";
    }

    private volatile boolean isRunning = true;

    /**
     * 测试规则，用于测试
     */
    private volatile boolean isTestRule;

    /**
     * 解析规则查询周期为1分钟
     */
    private volatile Long duration;

    /**
     * 规则sql
     */
    private volatile String ruleSql;

    private volatile DruidDataSource ruleSource;

    private volatile JdbcTemplate ruleJdbcTemplate;
}
