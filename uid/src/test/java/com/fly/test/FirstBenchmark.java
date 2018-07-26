package com.fly.test;

import com.fly.xid.sequence.IdWorker;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

//3个进程，8个线程，每次预热3次，每次5秒。执行5次，每次5秒，最后算出的值为平均时间，单位ms。

// Benchmark：同一个benchmark在多个线程之间共享实例。Group：同一个线程在同一个group里共享实例。group定义参考注解 Thread：不同线程之间的实例不共享
@State(Scope.Thread)
// 基准测试类型： Throughput吞吐量  AverageTime平均时间  SampleTime 随机取样  SingleShotTime ALL所有
@BenchmarkMode(Mode.Throughput)
// 一般选择秒、毫秒、微秒。
@OutputTimeUnit(TimeUnit.MILLISECONDS)
// 预热：iterations：预热的次数  time：每次预热的时间  timeUnit：时间的单位，默认秒 batchSize：批处理大小，每次操作调用几次方法
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
// 实际调用方法所需要配置的一些基本测试参数。可用于类或者方法上。参数和@Warmup一样。
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
// 进行 fork 的次数。可用于类或者方法上。如果 fork 数是2的话，则 JMH 会 fork 出两个进程来进行测试。
@Fork(3)
// 一般选择为cpu乘以2。如果配置了 Threads.MAX ，代表使用 Runtime.getRuntime().availableProcessors() 个线程。
@Threads(1)
public class FirstBenchmark {

    private final static Logger LOGGER = LoggerFactory.getLogger(FirstBenchmark.class);

    private IdWorker idWorker1 = null;
    private IdWorker idWorker2 = null;

    //@Setup主要实现测试前的初始化工作
    @Setup
    public void setup(){
        idWorker1 = new IdWorker(1L);
        idWorker2 = new IdWorker(2L);
    }

    //方法级注解，表示该方法是需要进行 benchmark 的对象
    @Benchmark
    public Long generatIdWork1() {
        return idWorker1.nextId();
    }

    @Benchmark
    public Long generatIdWork2() {
        return idWorker2.nextId();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FirstBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }


}
