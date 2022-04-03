package com.example.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import javax.swing.tree.TreeNode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;


public class ETL {
    public static void main(String[] args) throws Exception {
        //TODO 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 设置执行模式
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(4);

        //TODO 从文件中读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/simple.etl.csv");

        //TODO 过滤脏数据
        SingleOutputStreamOperator<String> filter = streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                String[] split = value.split(",");
                Boolean flag = true;
                try {
                    if (split[1] != "") {
                        flag = true;
                    }
                } catch (Exception e) {
//                    System.out.println("脏数据，过滤清除！");
                    flag = false;
                }
                return flag;
            }
        });

        //TODO 转换
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SingleOutputStreamOperator<Tuple3<String, Long ,Long>> map = filter.map(new MapFunction<String, Tuple3<String, Long ,Long>>() {
            @Override
            public Tuple3 map(String line) throws ParseException {
                String[] split = line.split(",");
                Tuple3<String, Long ,Long> Tuple3 = new Tuple3<>();
                Date date = simpleDateFormat.parse(split[0]);
                Tuple3.f0 = split[8].split(":")[1];
                Tuple3.f1 = date.getTime();
                Tuple3.f2 = 1l;
                return Tuple3;
            }
        });

        //TODO 设置事件时间并设置WM
        SingleOutputStreamOperator<Tuple3<String, Long ,Long>> tuple3Stream =
                map.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long ,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long ,Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long ,Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                }));

        //TODO 分组开窗聚合
//        SingleOutputStreamOperator<Tuple3<String, Long, Long>> result = tuple3Stream.keyBy(tuple3 -> tuple3.f0)
//                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//                .reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
//                    @Override
//                    public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> value1, Tuple3<String, Long, Long> value2) throws Exception {
//                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
//                    }
//                });
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> result = tuple3Stream.keyBy(tuple3 -> tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .sideOutputLateData(new OutputTag<Tuple3<String, Long, Long>>("timeout"){})
                .process(new ProcessWindowFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>, String, TimeWindow>.Context context, Iterable<Tuple3<String, Long, Long>> elements, Collector<Tuple3<String, Long, Long>> out) throws Exception {
                        long sum = 0L;
                        for (Tuple3<String, Long, Long> t : elements) {
                            sum += t.f2;
                        }
                        out.collect(Tuple3.of(key, context.window().getStart() ,sum));
                    }

                });

        result.getSideOutput(new OutputTag<Tuple3<String, Long, Long>>("timeout"){}).print("timeout");

        result.map(line->{
            System.out.println("TimeWindow:"+line.f1+" key:"+line.f0+" count:"+line.f2);
            return null;
        });

        env.execute();
    }
}
