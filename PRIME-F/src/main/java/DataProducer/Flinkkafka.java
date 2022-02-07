package DataProducer;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;


public class Flinkkafka {
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		
		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer011("mytopic", new SimpleStringSchema(), properties));
		
		// print() will write the contents of the stream to the TaskManager's standard out stream
		// the rebelance call is causing a repartitioning of the data so that all machines
		// see the messages (for example in cases when "num kafka partitions" < "num flink operators"
		messageStream.rebalance().map(new MapFunction<String, String>() {
			private static final long serialVersionUID = -6867736771747690202L;

			@Override
			public String map(String value) throws Exception {
				return "Kafka and Flink says: " + value;
			}
		}).print();

		env.execute();

        // the port to connect to
//        final int port;
//        try {
//            final ParameterTool params = ParameterTool.fromArgs(args);
//            port = params.getInt("port");
//        } catch (Exception e) {
//            System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
//            return;
//        }
//
//        // get the execution environment
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // get input data by connecting to the socket
//        DataStream<String> text = env.socketTextStream("localhost", port, "\n");
		
//		Properties properties = new Properties();
//		properties.setProperty("bootstrap.servers", "localhost:9092");
//		// only required for Kafka 0.8
////		properties.setProperty("zookeeper.connect", "localhost:2181");
//		properties.setProperty("group.id", "test");
//		DataStream<String> stream = env
//			.addSource(new FlinkKafkaConsumer08<>("topic", new SimpleStringSchema(), properties));

//        // parse the data, group it, window it, and aggregate the counts
//        DataStream<WordWithCount> windowCounts = text
//            .flatMap(new FlatMapFunction<String, WordWithCount>() {
//
//				public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
//					for (String word : value.split("\\s")) {
//                        out.collect(new WordWithCount(word, 1L));
//                    }
//					
//				}
//            })
//            .keyBy("word")
//            .timeWindow(Time.seconds(5), Time.seconds(1))
//            .reduce(new ReduceFunction<WordWithCount>() {
//
//				public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
//					return new WordWithCount(a.word, a.count + b.count);
//				}
//                
//            });
//
//        // print the results with a single thread, rather than in parallel
//        windowCounts.print().setParallelism(1);
//
//        env.execute("Socket Window WordCount");
    }

    // Data type for words with count
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
