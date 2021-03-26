package com.danier.flink.study.model.cep;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @Date 2020/11/30 16:30
 * @Author danier[xiaohui@mgtv.com]
 * @CopyRight : DataTeam @ MGTV Inc
 * @Desc:
 */
@Setter
@Getter
@ToString
public class LoginEventVo implements Serializable {

    private String userId;
    private String ip;
    private String type;
    private Long timestamp;

    public LoginEventVo(String userId, String ip, String type, Long timestamp) {
        this.userId = userId;
        this.ip = ip;
        this.type = type;
        this.timestamp = timestamp;
    }
}
