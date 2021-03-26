package com.danier.flink.study.model.cep;

import java.io.Serializable;

/**
 * @Date 2020/11/30 16:30
 * @Author danier[xiaohui@mgtv.com]
 * @CopyRight : DataTeam @ MGTV Inc
 * @Desc:
 */
public class LoginEventVo implements Serializable {
    private String userId;
    private String ip;
    private String type;

    public LoginEventVo(String userId, String ip, String type) {
        this.userId = userId;
        this.ip = ip;
        this.type = type;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "{\"_class\":\"LoginEventVo\", " +
                "\"userId\":" + (userId == null ? "null" : "\"" + userId + "\"") + ", " +
                "\"ip\":" + (ip == null ? "null" : "\"" + ip + "\"") + ", " +
                "\"type\":" + (type == null ? "null" : "\"" + type + "\"") +
                "}";
    }
}
