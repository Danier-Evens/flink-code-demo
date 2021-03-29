package com.danier.flink.study.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @Date 2020/11/30 16:30
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc:
 */
@Setter
@Getter
@ToString
public class LoginEventVo extends AbstractBasicVo implements Serializable {

    private String userId;
    private String ip;
    private String type;
    private Long timestamp;
    private Integer ruleId;

    public LoginEventVo buildUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public LoginEventVo buildIp(String ip) {
        this.ip = ip;
        return this;
    }

    public LoginEventVo buildType(String type) {
        this.type = type;
        return this;
    }

    public LoginEventVo buildTimestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public LoginEventVo buildRuleId(Integer ruleId) {
        this.ruleId = ruleId;
        return this;
    }


    public static LoginEventVo build() {
        return new LoginEventVo();
    }


    private LoginEventVo() {

    }

    @Override
    public long extractTimestamp() {
        return this.timestamp;
    }
}
