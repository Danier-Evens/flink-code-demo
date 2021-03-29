package com.danier.flink.study.model;

/**
 * @Date 2021/3/26 6:31 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc:
 */
public abstract class AbstractBasicVo {

    public abstract long extractTimestamp();

    /**
     * @return 维度
     */
    public Object dimension() {
        // TODO 子类按需overwrite this function
        return null;
    }

    /**
     * @return 度量
     */
    public Object measure() {
        // TODO 子类按需overwrite this function
        return null;
    }
}
