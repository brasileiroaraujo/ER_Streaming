package PRIMEbigdata;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import DataStructures.BlockStructureGraph;
import DataStructures.EntityNodeGraph;
import DataStructures.EntityProfile;
import DataStructures.TupleSimilarity;

//localhost:9092 localhost:2181 20 200 20 outputs/
public class PRIMEBigdataMainNoisy {
	
	private IntCounter numLines = new IntCounter();
	
	public static void main(String[] args) throws Exception {
		
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("flink-master", 8081, "/home/user/udfs.jar");
		env.setParallelism(Integer.parseInt(args[6]));
//		ExecutionEnvironment env = ExecutionEnvironment
//		        .createRemoteEnvironment("flink-master", 8081, "/home/user/udfs.jar");

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", args[0]);
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", args[1]);
		properties.setProperty("group.id", "test");
		properties.setProperty("enable.auto.commit", "true");
		properties.setProperty("auto.offset.reset", "earliest");
		//fast session timeout makes it more fun to play with failover
//		properties.setProperty("session.timeout.ms", "10000");
		//These buffer sizes seem to be needed to avoid consumer switching to
		//a mode where it processes one bufferful every 5 seconds with multiple
		//timeouts along the way.  No idea why this happens.
		//some properties was based in this flink project https://github.com/big-data-europe/pilot-sc4-flink-kafka-consumer/blob/master/src/main/resources/consumer.props
		properties.setProperty("fetch.min.bytes", "50000");
		properties.setProperty("receive.buffer.bytes", "10000000");//1000000
		properties.setProperty("max.partition.fetch.bytes", "50000000");//5000000
		
		
		DataStream<String> lines = env.addSource(new FlinkKafkaConsumer011<String>("mytopic", new SimpleStringSchema(), properties));
		
		DataStream<EntityProfile> entities = lines.rebalance().map(s -> new EntityProfile(s, true));
		
		//Receive the entities and extract the token from the attribute values.
		DataStream<Tuple2<Integer, EntityNodeGraph>> entitiesTokens = entities.rebalance().flatMap(new FlatMapFunction<EntityProfile, Tuple2<Integer, EntityNodeGraph>>() {
			
			@Override
			public void flatMap(EntityProfile e, Collector<Tuple2<Integer, EntityNodeGraph>> output) throws Exception {
//				Set<Integer> cleanTokens = new HashSet<Integer>();
//
//				for (Attribute att : e.getAttributes()) {
//					Set<String> tokens = generateTokens(att.getValue());
//					for (String string : tokens) {
//						cleanTokens.add(string.hashCode());
//					}
//				}
				
//				if ((se.isSource() && se.getKey() == 514) || (!se.isSource() && se.getKey() == 970)) {
//					System.out.println();
//				}
				
//				if (e.getKey() == 16228 || e.getKey() == 13754) {
//					System.out.println();
//				}
				
//				if ((e.getKey() == 577 && e.isSource()) || (e.getKey() == 148 && !e.isSource()) || (e.getKey() == 1634 && !e.isSource()) || (e.getKey() == 11998 && !e.isSource())) {
//					System.out.println(e.getKey() + " - " + e.getSetOfTokens());
//				}
//				System.out.println(e.getKey() + " - " + e.isSource() + " - " + e.getSetOfTokens().size());
				
				for (Integer tk : e.getSetOfTokens()) {
					EntityNodeGraph node = new EntityNodeGraph(tk, e.getKey(), e.getSetOfTokens(), e.isSource(), Integer.parseInt(args[2]), e.getIncrementID());
					output.collect(new Tuple2<Integer, EntityNodeGraph>(tk, node));
				}
				
			}
			
//			private Set<String> generateTokens(String string) {
//				if (string.length() > 19 && string.substring(0, 19).equals("http://dbpedia.org/")) {
//					String[] uriPath = string.split("/");
//					string = uriPath[uriPath.length-1];
//				}
//				
//				//To improve quality, use the following code
//				Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
//				Matcher m = p.matcher("");
//				m.reset(string);
//				String standardString = m.replaceAll("");
//				
//				KeywordGenerator kw = new KeywordGeneratorImpl();
//				return kw.generateKeyWords(standardString);
//			}
//			
//			private String[] generateTokensForBigData(String string) {
//				if (string.length() > 19 && string.substring(0, 19).equals("http://dbpedia.org/")) {
//					String[] uriPath = string.split("/");
//					string = uriPath[uriPath.length-1];
//				}
//				
//				return string.split("[\\W_]");
//			}
			
			
		});
		
		
		//Applies the token as a key.
		WindowedStream<Tuple2<Integer, EntityNodeGraph>, Tuple, TimeWindow> tokenKeys = 
				entitiesTokens.rebalance().keyBy(0).timeWindow(Time.seconds(Integer.parseInt(args[3])), Time.seconds(Integer.parseInt(args[4])));//define the window
		
		
		//Group the entities with the same token (blocking using the token as a key).
		SingleOutputStreamOperator<BlockStructureGraph> tokenBlocks = tokenKeys.aggregate(new AggregateFunction<Tuple2<Integer,EntityNodeGraph>, BlockStructureGraph, BlockStructureGraph>() {

			@Override
			public BlockStructureGraph add(Tuple2<Integer, EntityNodeGraph> entity, BlockStructureGraph acc) {
				BlockStructureGraph block = new BlockStructureGraph();
				block.setId(entity.f0);
				block.merge(acc);
				block.setCurrentIncrement(entity.f1.getIncrementId());
				if (entity.f1.isSource()) {
					block.addInSource(entity.f1);
				} else {
					block.addInTarget(entity.f1);
				}
				return block;
			}

			@Override
			public BlockStructureGraph createAccumulator() {
				return new BlockStructureGraph();
			}

			@Override
			public BlockStructureGraph getResult(BlockStructureGraph acc) {
				return acc;
			}

			@Override
			public BlockStructureGraph merge(BlockStructureGraph block1, BlockStructureGraph block2) {
				BlockStructureGraph block = new BlockStructureGraph();
				block.merge(block1);
				block.merge(block2);
				
				return block;
			}

		});
		
		
		
		//Remove the blocks with a huge number of entities (filtering block).
//		SingleOutputStreamOperator<BlockStructure> filteredTokenBlocks = tokenBlocks.rebalance().filter(new FilterFunction<BlockStructure>() {
//			
//			@Override
//			public boolean filter(BlockStructure b) throws Exception {
//				boolean filterActive = Boolean.parseBoolean(args[7]);
//				if (filterActive) {
//					int filterSize = Integer.parseInt(args[8]);
//					return b.size() < filterSize;
//				} else {
//					return true;
//				}
//			}
//		});
		
		
		SingleOutputStreamOperator<Tuple2<Integer, EntityNodeGraph>> entitySimilarities = tokenBlocks.rebalance().flatMap(new FlatMapFunction<BlockStructureGraph, Tuple2<Integer, EntityNodeGraph>>() {

			@Override
			public void flatMap(BlockStructureGraph block, Collector<Tuple2<Integer, EntityNodeGraph>> output) throws Exception {
				for (EntityNodeGraph eSource : block.getFromSource()) {
					for (EntityNodeGraph eTarget : block.getFromTarget()) {
//						if (eSource.getId() == 16228) {
//							System.out.println();
//						}
//						if (eSource.getIncrementId() == block.getCurrentIncrement() || eTarget.getIncrementId() == block.getCurrentIncrement()) {
							double sim = calculateSimilarity(block.getId(), eSource.getBlocks(), eTarget.getBlocks());
							if (sim >= 0) {
								eSource.addNeighbor(new TupleSimilarity(eTarget.getId(), sim));
							}
//						} 
//						else {
//							System.out.println();
//						}
					}
					
					output.collect(new Tuple2<Integer, EntityNodeGraph>(eSource.getId(), eSource));
				}
				
			}
			
			private double calculateSimilarity(Integer blockKey, Set<Integer> ent1, Set<Integer> ent2) {
				int maxSize = Math.min(ent1.size() - 1, ent2.size() - 1);//ent1.size() - 1;
				Set<Integer> intersect = new HashSet<Integer>(ent1);
				intersect.retainAll(ent2);

				// MACOBI strategy
				if (!Collections.min(intersect).equals(blockKey)) {
					return -1;
				}

				if (maxSize > 0) {
					double x = (double) intersect.size() / maxSize;
					return x;
				} else {
					return 0;
				}
			}
		});
		
		
		
		WindowedStream<Tuple2<Integer, EntityNodeGraph>, Tuple, TimeWindow> entityNeighborhood = 
				entitySimilarities.keyBy(0).timeWindow(Time.seconds(Integer.parseInt(args[4])));
		
		
		SingleOutputStreamOperator<EntityNodeGraph> graph = entityNeighborhood.aggregate(new AggregateFunction<Tuple2<Integer,EntityNodeGraph>, EntityNodeGraph, EntityNodeGraph>() {

			@Override
			public EntityNodeGraph add(Tuple2<Integer, EntityNodeGraph> entity, EntityNodeGraph acc) {
				EntityNodeGraph newEntity = new EntityNodeGraph();
				newEntity.setId(entity.f1.getId());
				newEntity.setMaxNumberOfNeighbors(Integer.parseInt(args[2]));
				newEntity.addAllNeighbors(entity.f1.getNeighbors());
				newEntity.addAllNeighbors(acc.getNeighbors());
				return newEntity;
			}

			@Override
			public EntityNodeGraph createAccumulator() {
				return new EntityNodeGraph();
			}

			@Override
			public EntityNodeGraph getResult(EntityNodeGraph acc) {
				return acc;
			}

			@Override
			public EntityNodeGraph merge(EntityNodeGraph e1, EntityNodeGraph e2) {
				EntityNodeGraph newEntity = new EntityNodeGraph();
				newEntity.setId(e1.getId());
				newEntity.setMaxNumberOfNeighbors(Integer.parseInt(args[2]));
				newEntity.addAllNeighbors(e1.getNeighbors());
				newEntity.addAllNeighbors(e2.getNeighbors());
				return newEntity;
			}
		});
		
		
		
		//Execute a pruning of the neighbors
		DataStreamSink<String> output = graph.rebalance().map(new MapFunction<EntityNodeGraph, String>() {
			@Override
			public String map(EntityNodeGraph node) throws Exception {
//				if (node.getId() == 30) {
//					System.out.println();
//				}
				node.pruningWNP();
				return node.getId() + ">" + node.toString();
			}
		}).writeAsText(args[5]);
		
//		FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
//				args[0],            // broker list
//		        "output",                  // target topic
//		        new SimpleStringSchema());
//		myProducer.setWriteTimestampToKafka(true);
//		
//		output.addSink(myProducer);
		
		env.execute();
	}
}
