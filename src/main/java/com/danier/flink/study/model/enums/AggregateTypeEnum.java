package com.danier.flink.study.model.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.stream.Stream;

/**
 * @Date 2021/3/26 5:43 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc:
 */
@Getter
@ToString
@AllArgsConstructor
public enum AggregateTypeEnum {

    COUNT(1, "求总数"),
    DISTINCT(2, "去重"),
    SUM(3, "求和"),
    AVG(4, "求平均值"),
    MAX(5, "求最大值"),
    MIN(6, "求最小值");

    private Integer type;
    private String value;

    public static AggregateTypeEnum getByType(Integer type) {
        return Stream.of(AggregateTypeEnum.values())
                .filter(e -> e.getType().equals(type))
                .findFirst()
                .orElse(null);
    }
}
