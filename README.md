# com.danier.flink.study.stream.BroadcastStreamApp

* 问题or功能：周期加载外部数据源作为广播流，与事件流进行connect，对事件流信息进行补充。
* 场景：风控等
* 备注：去重算法，利用bitmap算法实现。

# com.danier.flink.study.stream.SlidingWindowEnhancement

* 问题or功能：滑动窗口，需要设置滑动步长，当窗口size越大，步长越小，窗口重叠部分越多，事件分配到窗口，对state写存在写放大，写次数越多，会影响吞吐。 利用GlobalWindows + DeltaTrigger (
  1s间隔触发，可配置) + TimeEvictor（保留最近一个窗口size的数据进行统计）替代已有滑动窗口的实现。

# com.danier.flink.study.cep.LoginFail

* 问题or功能：利用cep，检测用户登录失败次数超过3次的用户，并将登录失败的ip进行输出。