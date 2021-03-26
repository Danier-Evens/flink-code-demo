package com.danier.flink.study.stream.function.process;

import com.danier.flink.study.model.RuleVo;
import com.danier.flink.study.model.LoginEventVo;
import com.danier.flink.study.model.LoginEventWithRuleVo;
import com.danier.flink.study.util.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @Date 2021/3/26 6:27 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc:
 */
@Slf4j
public class RuleBroadcastProcessFunction extends BroadcastProcessFunction<LoginEventVo, Map<Integer, RuleVo>, LoginEventWithRuleVo> {

    /**
     * 读取状态，并基于状态，处理事件流中的数据
     * 在这里，从上下文中获取状态，基于获取的状态，对事件流中的数据进行处理
     *
     * @param loginEventVo    事件流中的数据
     * @param readOnlyContext 上下文
     * @param collector       输出零条或多条数据
     * @throws Exception
     */
    @Override
    public void processElement(LoginEventVo loginEventVo, ReadOnlyContext readOnlyContext, Collector<LoginEventWithRuleVo> collector) throws Exception {
        //获取状态
        ReadOnlyBroadcastState<Void, Map<Integer, RuleVo>> broadcastState = readOnlyContext.getBroadcastState(Constant.BROADCAST_DESC);
        Map<Integer, RuleVo> broadcastStateRuleInfo = broadcastState.get(null);

        RuleVo ruleVo = broadcastStateRuleInfo.get(loginEventVo.getRuleId());
        if (ruleVo != null) {
            LoginEventWithRuleVo outVo = new LoginEventWithRuleVo();
            outVo.setRule(ruleVo);
            outVo.setEvent(loginEventVo);
            collector.collect(outVo);
        }
    }

    @Override
    public void processBroadcastElement(Map<Integer, RuleVo> integerRuleVoMap, Context context, Collector<LoginEventWithRuleVo> collector) throws Exception {
        //获取状态
        BroadcastState<Void, Map<Integer, RuleVo>> broadcastState = context.getBroadcastState(Constant.BROADCAST_DESC);

        //清空状态
        broadcastState.clear();

        //更新状态
        broadcastState.put(null, integerRuleVoMap);
        log.info("##### processBroadcastElement 更新状态. rule size:{}", integerRuleVoMap.size());
    }
}
