package com.danier.flink.study.stream.time;

import com.danier.flink.study.model.ExtractTimeVo;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

import java.io.Serializable;

/**
 * @Date 2021/3/26 4:03 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc:
 */
public class LoginBoundedOutOfOrderness<T> extends BoundedOutOfOrdernessTimestampExtractor<T> implements Serializable {

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
    public long extractTimestamp(T extractTime) {
        if (extractTime instanceof ExtractTimeVo) {
            return ((ExtractTimeVo) extractTime).extractTimestamp();
        } else {
            return Long.MIN_VALUE;
        }
    }
}
