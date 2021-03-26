package com.danier.flink.study.stream.process;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @Date 2021/3/26 4:38 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc:
 */
public class GlobalWindowProcessFunction<T> extends ProcessWindowFunction<T, Tuple2<String, T>, String, GlobalWindow> {

    @Override
    public void process(String key, Context context, Iterable<T> iterable, Collector<Tuple2<String, T>> collector) throws Exception {
        collector.collect(new Tuple2(key, iterable.iterator().next()));
    }
}
