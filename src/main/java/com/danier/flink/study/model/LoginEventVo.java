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
@NoArgsConstructor
public class LoginEventVo extends ExtractTimeVo implements Serializable {

    private String userId;
    private String ip;
    private String type;
    private Long timestamp;
    private Integer ruleId;

    public LoginEventVo(String userId, String ip, String type, Long timestamp) {
        new LoginEventVo(userId, ip, type, timestamp, null);
    }

    public LoginEventVo(String userId, String ip, String type, Long timestamp, Integer ruleId) {
        this.userId = userId;
        this.ip = ip;
        this.type = type;
        this.timestamp = timestamp;
        this.ruleId = ruleId;
    }

    @Override
    public long extractTimestamp() {
        return this.timestamp;
    }
}
