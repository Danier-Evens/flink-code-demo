package com.danier.flink.study.util;

import com.danier.flink.study.model.RuleVo;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.Map;

/**
 * @Date 2021/3/26 6:45 下午
 * @Author danier[danierwei@gmail.com]
 * @CopyRight : coding @ Civil Private Organization Inc
 * @Desc:
 */
public class Constant {

    //  MapStateDescriptor定义了状态的名称、Key和Value的类型。这里，MapStateDescriptor中，key是Void类型，value是Map<Integer, RuleVo>类型。
    public final static MapStateDescriptor<Void, Map<Integer, RuleVo>> BROADCAST_DESC =
            new MapStateDescriptor<>("broadcastConfig", Types.VOID, Types.MAP(TypeInformation.of(Integer.class), TypeInformation.of(RuleVo.class)));
}
