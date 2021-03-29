package com.danier.flink.study.stream;

import com.alibaba.fastjson.JSONObject;
import com.danier.flink.study.model.LoginEventVo;
import com.danier.flink.study.model.LoginEventWithRuleVo;
import com.danier.flink.study.model.RuleVo;
import com.danier.flink.study.model.enums.AggregateTypeEnum;
import com.danier.flink.study.stream.function.TimeDeltaFunction;
import com.danier.flink.study.stream.function.agg.BitMapDistinctAggFunction;
import com.danier.flink.study.stream.function.agg.CountAggFunction;
import com.danier.flink.study.stream.function.process.RuleBroadcastProcessFunction;
import com.danier.flink.study.stream.process.GlobalWindowProcessFunction;
import com.danier.flink.study.stream.source.RuleSource;
import com.danier.flink.study.stream.time.LoginBoundedOutOfOrderness;
import com.danier.flink.study.util.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Map;
import java.util.Properties;

/**
 * @Date 2021/3/26 5:28 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc: 周期加载外部数据源作为广播流，与事件流进行connect，对事件流信息进行补充。
 * <p>
 * 运行：bin/flink run -c com.danier.flink.study.stream.BroadcastStreamApp ~/work/code/study/flink-code-demo/target/flink-code-demo-0.1.jar --topic test
 */
public class BroadcastStreamApp {
    private static final String jobName = BroadcastStreamApp.class.getSimpleName();
    private static ParameterTool params;

    public static void main(String[] args) throws Exception {
        params = ParameterTool.fromArgs(args);

        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        int parallelism = params.getInt("parallelism", 1);
        env.setParallelism(parallelism);


        Time windowSize = Time.seconds(5l); // 窗口大小
        Time windowSlide = Time.seconds(1l); // 窗口滑动步长

        SingleOutputStreamOperator<LoginEventVo> kafkaStream = getKafkaStream(env);
        BroadcastStream<Map<Integer, RuleVo>> ruleBroadcastStream = getRuleBroadcastStream(env);
        SingleOutputStreamOperator<LoginEventWithRuleVo> connectStream = connectStream(kafkaStream, ruleBroadcastStream);

        // count算子
        SingleOutputStreamOperator countAggregate = connectStream.filter(r -> r.getRule() != null
                && AggregateTypeEnum.COUNT.equals(r.getRule().getAggregateType()))
                .keyBy(r -> r.dimension())
                .window(GlobalWindows.create())
                // 窗口触发计算前，进行剔除不满足时间范围的事件。
                .evictor(TimeEvictor.of(windowSize))
                // 触发器：按照windowSlide大小进行触发，计算方式为与上一次触发元素比较，计算时间差，判断是否大于windowSlide。
                .trigger(
                        DeltaTrigger.of(windowSlide.getUnit().toSeconds(windowSlide.getSize()),
                                new TimeDeltaFunction(),
                                TypeInformation.of(LoginEventWithRuleVo.class).createSerializer(env.getConfig())))
                .aggregate(new CountAggFunction(), new GlobalWindowProcessFunction());
        countAggregate.print();

        // 去重算子
        SingleOutputStreamOperator distinctAggregate = connectStream.filter(r -> r.getRule() != null
                && AggregateTypeEnum.DISTINCT.equals(r.getRule().getAggregateType()))
                .keyBy(r -> r.dimension())
                .window(GlobalWindows.create())
                // 窗口触发计算前，进行剔除不满足时间范围的事件。
                .evictor(TimeEvictor.of(windowSize))
                // 触发器：按照windowSlide大小进行触发，计算方式为与上一次触发元素比较，计算时间差，判断是否大于windowSlide。
                .trigger(
                        DeltaTrigger.of(windowSlide.getUnit().toSeconds(windowSlide.getSize()),
                                new TimeDeltaFunction(),
                                TypeInformation.of(LoginEventWithRuleVo.class).createSerializer(env.getConfig())))
                .aggregate(new BitMapDistinctAggFunction(), new GlobalWindowProcessFunction());
        distinctAggregate.print();

        // fixme 其他算子有需要可以跟count、dinstinct算子类似实现方式。。
        // 执行
        env.execute(jobName);
    }

    /**
     * @param env 环境
     * @return kafka source stream
     */
    private static final SingleOutputStreamOperator<LoginEventVo> getKafkaStream(StreamExecutionEnvironment env) {
        String topic = params.get("topic");
        Properties kafkaP = new Properties();
        kafkaP.setProperty("group.id", params.get("group.id", jobName));
        kafkaP.setProperty("bootstrap.servers", params.get("bootstrap.servers", "127.0.0.1:9092"));
        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), kafkaP);
        SingleOutputStreamOperator<LoginEventVo> stream = env.addSource(kafkaSource)
                .uid("kafka_" + jobName)
                .name("kafka_" + jobName)
                .map((msg) -> {
                    JSONObject json = JSONObject.parseObject(msg);
                    return LoginEventVo.build()
                            .buildUserId(json.getString("userId"))
                            .buildIp(json.getString("ip"))
                            .buildType(json.getString("type"))
                            .buildTimestamp(json.getLong("timestamp"))
                            .buildRuleId(json.getInteger("ruleId"));
                })
                .assignTimestampsAndWatermarks(new LoginBoundedOutOfOrderness());
        return stream;
    }


    /**
     * @return rule 广播流
     */
    private static final BroadcastStream<Map<Integer, RuleVo>> getRuleBroadcastStream(StreamExecutionEnvironment env) {
        long duration = params.getLong("duration", 60 * 1000L);
        boolean isTestRule = params.getBoolean("isTestRule", true);
        SingleOutputStreamOperator<Map<Integer, RuleVo>> ruleStream =
                env.addSource(new RuleSource(isTestRule, duration))
                        .uid("rule_" + jobName)
                        .name("rule_" + jobName)
                        .setParallelism(1);
        // 将配置流广播，形成BroadcastStream
        BroadcastStream<Map<Integer, RuleVo>> broadcast = ruleStream.broadcast(Constant.BROADCAST_DESC);
        return broadcast;
    }


    /**
     * connect kafka and rule stream
     *
     * @param kafkaStream         kafka stream
     * @param broadcastRuleStream rule boradcast stream
     * @return connect stream
     */
    private static final SingleOutputStreamOperator<LoginEventWithRuleVo> connectStream(SingleOutputStreamOperator<LoginEventVo> kafkaStream,
                                                                                        BroadcastStream<Map<Integer, RuleVo>> broadcastRuleStream) {
        // 事件流和广播的配置流连接，形成BroadcastConnectedStream
        BroadcastConnectedStream<LoginEventVo, Map<Integer, RuleVo>> connectedStream = kafkaStream.connect(broadcastRuleStream);

        //  对BroadcastConnectedStream应用process方法，根据配置(规则)处理事件
        SingleOutputStreamOperator<LoginEventWithRuleVo> kafkaConnectRuleStream =
                connectedStream.process(new RuleBroadcastProcessFunction())
                        .filter(r -> r != null)
                        .assignTimestampsAndWatermarks(new LoginBoundedOutOfOrderness());
        return kafkaConnectRuleStream;
    }
}
