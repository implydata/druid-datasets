package p1;


import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.*;
 


public class KafkaDemo 
{
	public static void main(String[] args) throws Exception
    {
	final StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
	StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

	
	KafkaSource<String> source1 = KafkaSource.<String>builder()
		    .setBootstrapServers("localhost:9092")
		    .setTopics("orders")
		    .setGroupId("my-group")
		    .setValueOnlyDeserializer(new SimpleStringSchema())
		    .build();
	
	KafkaSource<String> source2 = KafkaSource.<String>builder()
		    .setBootstrapServers("localhost:9092")
		    .setTopics("orderdetails")
		    .setGroupId("my-group1")
		    .setValueOnlyDeserializer(new SimpleStringSchema())
		    .build();

	DataStream<String> text1 = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Kafka Source1");
	DataStream<String> text2 = env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Kafka Source2");

	DataStream<Tuple7<Integer,String,String,String,String,String,String>> orders=text1.map(new MapFunction<String, Tuple7<Integer,String,String,String,String,String,String>>()
	{
		public Tuple7<Integer,String,String,String,String,String,String> map(String value)

		{
			String[] words = value.split(",");                                                 // words = [ {1} {John}]
			return new Tuple7<Integer,String,String,String,String,String,String>(Integer.parseInt(words[0]), words[1],words[2], words[3],words[4], words[5],words[6]);
		}
		});
	
	DataStream<Tuple5<Integer,String,String,String,String>> details=text2.map(new MapFunction<String, Tuple5<Integer,String,String,String,String>>()
	{
		public Tuple5<Integer,String,String,String,String> map(String value)

		{
			String[] words = value.split(",");                                                 // words = [ {1} {John}]
			return new Tuple5<Integer,String,String,String,String>(Integer.parseInt(words[0]), words[1],words[2], words[3],words[4]);
		}
		});
	
	Table inputTable = tableEnv.fromDataStream(orders);
	tableEnv.createTemporaryView("orders", inputTable);
	Table inputTable1 = tableEnv.fromDataStream(details);
	tableEnv.createTemporaryView("details", inputTable1);
	Table resultTable = tableEnv.sqlQuery("SELECT orders.f0,orders.f1,orders.f2,orders.f3,orders.f4,orders.f5,orders.f6,details.f1,details.f2,details.f3,details.f4 FROM orders inner join details on orders.f0=details.f0");
	
	@SuppressWarnings("deprecation")
	TupleTypeInfo<Tuple11<Integer,String,String,String,String,String,String,String,String,String,String>> tupleType = new TupleTypeInfo<>(Types.INT(), Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING());
	@SuppressWarnings("deprecation")
	DataStream<Tuple11<Integer,String,String,String,String,String,String,String,String,String,String>> dsTuple = tableEnv.toAppendStream(resultTable, tupleType);
	//dsTuple.writeAsCsv("/Users/vijaynarayanan/Documents/devrel/kafka1.txt");
	
	DataStream<String> joined=dsTuple.map(new MapFunction<Tuple11<Integer,String,String,String,String,String,String,String,String,String,String>,String>()
	{
		public  String map(Tuple11<Integer,String,String,String,String,String,String,String,String,String,String> value)

		{
			String words = value.f0.toString()+","+value.f1+","+value.f2+","+value.f3+","+value.f4+","+value.f5+","+value.f6+","+value.f7+","+value.f8+","+value.f9+","+value.f10;                                             // words = [ {1} {John}]
			return words;
		}
		});
	KafkaSink<String> sink = KafkaSink.<String>builder()
	        .setBootstrapServers("localhost:9092")
	        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
	            .setTopic("joineddata")
	            .setValueSerializationSchema(new SimpleStringSchema())
	            .build()
	        )
	        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
	        .build();

     joined.sinkTo(sink);
	
	env.execute("Kafka Example");
    }
	
}

