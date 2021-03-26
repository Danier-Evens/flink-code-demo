package com.danier.flink.study.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Date 2021/3/26 5:49 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc: 对事件流补充统计规则
 */
@Data
@NoArgsConstructor
public class LoginEventWithRuleVo extends ExtractTimeVo implements Serializable {

    private LoginEventVo event;
    private RuleVo rule;

    @Override
    public long extractTimestamp() {
        return this.event.getTimestamp();
    }
}
