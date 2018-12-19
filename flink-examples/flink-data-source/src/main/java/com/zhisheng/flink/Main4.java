package com.zhisheng.flink;

import com.alibaba.fastjson.JSON;
import com.zhisheng.flink.model.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * Desc:
 * weixin: zhisheng_tian
 * blog: http://www.54tianzhisheng.cn/
 */
public class Main4 {
    public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", "metric-group");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "latest");

		SingleOutputStreamOperator<Student> data = env.addSource(new FlinkKafkaConsumer011<>(
			"student",
			new SimpleStringSchema(),
			props)).setParallelism(1)
			.map(string -> JSON.parseObject(string, Student.class));

		data.keyBy(1)
			.timeWindow(Time.minutes(1)) //tumbling time window 每分钟统计一次数量和
			.sum(1);

		data.keyBy(1)
			.timeWindow(Time.minutes(1), Time.seconds(30)) //sliding time window 每隔 30s 统计过去一分钟的数量和
			.sum(1);


		data.keyBy(1)
			.countWindow(100)
			.sum(1);

		data.keyBy(1)
			.countWindow(100, 10)
			.sum(1);


    }
}
