package com.danier.flink.study.stream.time;

import com.danier.flink.study.model.cep.LoginEventVo;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

import java.io.Serializable;

/**
 * @Date 2021/3/26 4:03 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc:
 */
public class LoginBoundedOutOfOrderness extends BoundedOutOfOrdernessTimestampExtractor<LoginEventVo> implements Serializable {

    /**
     * @param maxOutOfOrderness 最大允许乱序时间 单位ms
     */
    public LoginBoundedOutOfOrderness(long maxOutOfOrderness) {
        super(Time.milliseconds(maxOutOfOrderness));
    }

    public LoginBoundedOutOfOrderness() {
        this(2000l);
    }

    @Override
    public long extractTimestamp(LoginEventVo loginEventVo) {
        return loginEventVo.getTimestamp();
    }
}
