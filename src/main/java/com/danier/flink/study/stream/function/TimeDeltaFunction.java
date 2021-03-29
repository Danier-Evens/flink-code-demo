package com.danier.flink.study.stream.function;

import com.danier.flink.study.model.AbstractBasicVo;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;

/**
 * @Date 2021/3/26 3:28 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc:
 */
public class TimeDeltaFunction<T> implements DeltaFunction<T> {

    @Override
    public double getDelta(T oldDataPoint, T newDataPoint) {
        return (double) ((AbstractBasicVo) newDataPoint).extractTimestamp() -
                (double) ((AbstractBasicVo) oldDataPoint).extractTimestamp();
    }
}
