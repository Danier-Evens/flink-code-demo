package com.danier.flink.study.stream.function;

import com.danier.flink.study.model.cep.LoginEventVo;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;

/**
 * @Date 2021/3/26 3:28 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc:
 */
public class TimeDeltaFunction implements DeltaFunction<LoginEventVo> {

    @Override
    public double getDelta(LoginEventVo oldDataPoint, LoginEventVo newDataPoint) {
        return (double) newDataPoint.getTimestamp() - (double) oldDataPoint.getTimestamp();
    }
}
