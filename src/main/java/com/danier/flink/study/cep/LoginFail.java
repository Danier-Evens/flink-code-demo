package com.danier.flink.study.cep;

import com.danier.flink.study.model.LoginEventVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Date 2020/11/24 11:03
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc: 检测用户登录失败次数超过3次的用户(严格连续)，并将登录失败的ip进行输出。
 * 运行： bin/flink run -c com.danier.flink.study.cep.LoginFail ~/work/code/study/flink-code-demo/target/flink-code-demo-0.1.jar
 * 输出：
 * 2> 192.168.0.1 -> 192.168.0.2 -> 192.168.0.3
 * 2> 192.168.0.2 -> 192.168.0.3 -> 192.168.0.4
 */
@Slf4j
public class LoginFail {

    private static final String jobName = LoginFail.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<LoginEventVo> loginEventStream = env.fromCollection(Arrays.asList(
                LoginEventVo.build().buildUserId("1").buildIp("192.168.0.1").buildType("fail"),
                LoginEventVo.build().buildUserId("1").buildIp("192.168.0.1").buildType("success"),
                LoginEventVo.build().buildUserId("1").buildIp("192.168.0.2").buildType("fail"),
                LoginEventVo.build().buildUserId("1").buildIp("192.168.0.3").buildType("fail"),
                LoginEventVo.build().buildUserId("1").buildIp("192.168.0.4").buildType("fail"),
                LoginEventVo.build().buildUserId("2").buildIp("192.168.10.10").buildType("fail"),
                LoginEventVo.build().buildUserId("2").buildIp("192.168.10.10").buildType("fail"),
                LoginEventVo.build().buildUserId("2").buildIp("192.168.10.10").buildType("success")
        ));

        Pattern<LoginEventVo, LoginEventVo> loginFailPattern = Pattern.<LoginEventVo>
                begin("begin")
                .where(new IterativeCondition<LoginEventVo>() {
                    @Override
                    public boolean filter(LoginEventVo loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("fail");
                    }
                })
                .times(3);

        PatternStream<LoginEventVo> pattern =
                CEP.pattern(loginEventStream.keyBy(LoginEventVo::getUserId), loginFailPattern);

        SingleOutputStreamOperator<Object> resultStream = pattern.flatSelect(new PatternFlatSelectFunction<LoginEventVo, Object>() {
            @Override
            public void flatSelect(Map<String, List<LoginEventVo>> values, Collector<Object> collector) throws Exception {
                String matchPatternEvent = values.values()
                        .stream()
                        .flatMap(e -> e.stream())
                        .map(LoginEventVo::getIp)
                        .collect(Collectors.joining(" -> "));
                collector.collect(matchPatternEvent);
            }
        });

        resultStream.print();

        env.execute(jobName);
    }
}
