package com.danier.flink.study.stream.function.agg;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Date 2021/3/26 4:09 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc: 求总数
 */
public class CountAggFunction<T> implements AggregateFunction<T, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(T value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
