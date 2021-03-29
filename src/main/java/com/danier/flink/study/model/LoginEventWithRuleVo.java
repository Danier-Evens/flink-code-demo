package com.danier.flink.study.model;

import com.alibaba.fastjson.JSONObject;
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
public class LoginEventWithRuleVo extends AbstractBasicVo implements Serializable {

    private RuleVo rule;
    private LoginEventVo event;

    @Override
    public long extractTimestamp() {
        return this.event.getTimestamp();
    }

    @Override
    public Object measure() {
        if (event == null)
            return null;

        JSONObject json =
                JSONObject.parseObject(JSONObject.toJSONString(event));
        String measureField = rule.getMeasureField();
        String measureValue = json.getString(measureField);
        return measureValue;
    }

    @Override
    public String dimension() {
        if (event == null)
            return null;

        String[] fields = rule.getDimensionFields();
        if (fields == null)
            return null;

        JSONObject json =
                JSONObject.parseObject(JSONObject.toJSONString(event));
        StringBuffer b = new StringBuffer();
        for (String f : fields) {
            b.append(json.getString(f));
        }
        return b.toString();
    }
}
