package com.danier.flink.study.stream;

import com.alibaba.fastjson.JSONObject;
import com.danier.flink.study.model.cep.LoginEventVo;
import com.danier.flink.study.stream.function.TimeDeltaFunction;
import com.danier.flink.study.stream.function.agg.CountAggFunction;
import com.danier.flink.study.stream.process.GlobalWindowProcessFunction;
import com.danier.flink.study.stream.time.LoginBoundedOutOfOrderness;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @Date 2021/3/26 3:14 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc: 背景：滑动窗口，需要设置滑动步长，当窗口size越大，步长越小，窗口重叠部分越多，事件分配到窗口，对state写存在写放大，写次数越多，会影响吞吐。
 * 替代方案：GlobalWindows + DeltaTrigger (1s间隔触发，可配置) + TimeEvictor（保留最近一个窗口size的数据进行统计）
 * 注意：一定要使用EventTime，否则TimeEvictor不生效（TimestampedValue中timestamp字段为空）。
 * <p>
 * 运行：bin/flink run -c com.danier.flink.study.stream.SlidingWindowEnhancement ~/work/code/study/flink-code-demo/target/flink-code-demo-0.1.jar --topic test
 */
public class SlidingWindowEnhancement {

    public static void main(String[] args) throws Exception {
        String jobName = SlidingWindowEnhancement.class.getSimpleName();
        ParameterTool params = ParameterTool.fromArgs(args);

        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Time windowSize = Time.seconds(5l); // 窗口大小
        Time windowSlide = Time.seconds(1l); // 窗口滑动步长

        // 构建数据流
        String topic = params.get("topic");
        int parallelism = params.getInt("parallelism", 1);
        Properties kafkaP = new Properties();
        kafkaP.setProperty("group.id", params.get("group.id", jobName));
        kafkaP.setProperty("bootstrap.servers", params.get("bootstrap.servers", "127.0.0.1:9092"));
        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), kafkaP);
        SingleOutputStreamOperator<Long> aggregate = env.addSource(kafkaSource)
                .setParallelism(parallelism)
                .map((msg) -> JSONObject.parseObject(msg, LoginEventVo.class))
                .assignTimestampsAndWatermarks(new LoginBoundedOutOfOrderness())
                .keyBy(LoginEventVo::getUserId)
                .window(GlobalWindows.create())
                // 窗口触发计算前，进行剔除不满足时间范围的事件。
                .evictor(TimeEvictor.of(windowSize))
                // 触发器：按照windowSlide大小进行触发，计算方式为与上一次触发元素比较，计算时间差，判断是否大于windowSlide。
                .trigger(
                        DeltaTrigger.of(windowSlide.getUnit().toSeconds(windowSlide.getSize()),
                                new TimeDeltaFunction(),
                                TypeInformation.of(LoginEventVo.class).createSerializer(env.getConfig())))
                .aggregate(new CountAggFunction(), new GlobalWindowProcessFunction());

        // 打印
        aggregate.print();

        // 执行
        env.execute(jobName);
    }
}
