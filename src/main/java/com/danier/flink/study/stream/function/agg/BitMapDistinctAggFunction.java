package com.danier.flink.study.stream.function.agg;

import com.danier.flink.study.model.AbstractBasicVo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * @Date 2021/3/29 9:25 上午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc: 使用bitmap实现的去重方法
 */
public class BitMapDistinctAggFunction<T extends AbstractBasicVo> implements AggregateFunction<T, Roaring64NavigableMap, Long> {

    @Override
    public Roaring64NavigableMap createAccumulator() {
        return new Roaring64NavigableMap();
    }

    @Override
    public Roaring64NavigableMap add(T t, Roaring64NavigableMap accumulator) {
        // fixme 获取对象hashcode，会存在hash碰撞，去重存在一定误差。
        //  对误差要求严格的场景建议使用类型为long、int等类型的字段当做度量字段。
        accumulator.add(t.measure().hashCode());
        return accumulator;
    }

    @Override
    public Long getResult(Roaring64NavigableMap accumulator) {
        return accumulator.getLongCardinality();
    }

    @Override
    public Roaring64NavigableMap merge(Roaring64NavigableMap a, Roaring64NavigableMap b) {
        a.or(b);
        return a;
    }
}
