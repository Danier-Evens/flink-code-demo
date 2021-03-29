package com.danier.flink.study.model;

import lombok.Data;
import lombok.AllArgsConstructor;
import com.danier.flink.study.model.enums.AggregateTypeEnum;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Date 2021/3/26 5:38 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc: 规则
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RuleVo implements Serializable {

    private int ruleId;
    // 度量字段
    private String measureField;
    // 维度字段
    private String[] dimensionFields;
    // 聚合类型
    private AggregateTypeEnum aggregateType;
}
