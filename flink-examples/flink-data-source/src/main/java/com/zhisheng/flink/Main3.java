package com.zhisheng.flink;

import com.alibaba.fastjson.JSON;
import com.zhisheng.flink.model.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * Desc:
 * weixin: zhisheng_tian
 * blog: http://www.54tianzhisheng.cn/
 */
public class Main3 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer011<>(
                "student",
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> JSON.parseObject(string, Student.class));

        student.keyBy(new KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student value) throws Exception {
                return value.age;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));


        /*SingleOutputStreamOperator<Student> reduce = student.keyBy(new KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student value) throws Exception {
                return value.age;
            }
        }).reduce(new ReduceFunction<Student>() {
            @Override
            public Student reduce(Student value1, Student value2) throws Exception {
                Student student1 = new Student();
                student1.name = value1.name + value2.name;
                student1.id = (value1.id + value2.id) / 2;
                student1.password = value1.password + value2.password;
                student1.age = (value1.age + value2.age) / 2;
                return student1;
            }
        });
        reduce.print();*/

       /* KeyedStream<Student, Integer> keyBy = student.keyBy(new KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student value) throws Exception {
                return value.age;
            }
        });
        keyBy.print();*/

        /*SingleOutputStreamOperator<Student> filter = student.filter(new FilterFunction<Student>() {
            @Override
            public boolean filter(Student value) throws Exception {
                if (value.id > 95) {
                    return true;
                }
                return false;
            }
        });
        filter.print();*/

        /*SingleOutputStreamOperator<Student> flatMap = student.flatMap(new FlatMapFunction<Student, Student>() {
            @Override
            public void flatMap(Student value, Collector<Student> out) throws Exception {
                if (value.id % 2 == 0) {
                    out.collect(value);
                }
            }
        });
        flatMap.print();*/
        

        /*SingleOutputStreamOperator<Student> map = student.map(new MapFunction<Student, Student>() {
            @Override
            public Student map(Student value) throws Exception {
                Student s1 = new Student();
                s1.id = value.id;
                s1.name = value.name;
                s1.password = value.password;
                s1.age = value.age + 5;
                return s1;
            }
        });
        map.print();*/

//        student.addSink(new SinkToMySQL());

//        student.addSink(new PrintSinkFunction<>());
        env.execute("Flink add sink");
    }
}
