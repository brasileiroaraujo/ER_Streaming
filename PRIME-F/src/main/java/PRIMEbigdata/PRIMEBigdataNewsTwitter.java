package PRIMEbigdata;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.jena.sparql.function.library.date;
import org.apache.lucene.analysis.StopAnalyzer;

import DataStructures.BlockStructure;
import DataStructures.EntityNode;
import DataStructures.TupleSimilarity2;
import FootballApi.MatchAPI;
import NewsAPI.NewsAPI;
import NewsAPI.R7API;
import TwitterApi.TwitterAPI;

//localhost:9092 localhost:2181 20 15 5 outputs/ 1 true
//kafka_host zookeper_host num_top-n window_size slice_size output_path num_nodes use_attribute_selection
public class PRIMEBigdataNewsTwitter {
	
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
		properties.setProperty("receive.buffer.bytes", "1000000");//1000000
		properties.setProperty("max.partition.fetch.bytes", "5000000");//5000000
		
		
		DataStream<String> linesFromAPI = env.addSource(new FlinkKafkaConsumer011<String>("mytopicNews", new SimpleStringSchema(), properties));
		
		DataStream<String> linesFromTwitter = env.addSource(new FlinkKafkaConsumer011<String>("mytopicTwitter", new SimpleStringSchema(), properties));
		
		DataStream<R7API> entitiesFromAPI = linesFromAPI.rebalance().map(s -> new R7API(s));
		
		DataStream<TwitterAPI> entitiesFromTwitter = linesFromTwitter.rebalance().map(s -> new TwitterAPI(s));
		
		
		//Receive the entities and extract the token from the attribute values.
		DataStream<Tuple2<String, EntityNode>> matchesTokens = entitiesFromAPI.rebalance().flatMap(new FlatMapFunction<R7API, Tuple2<String, EntityNode>>() {
			
			@Override
			public void flatMap(R7API e, Collector<Tuple2<String, EntityNode>> output) throws Exception {
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
				
				Set<String> allTokens = e.getAllTokens(Boolean.parseBoolean(args[7]));

				
				for (String tk : allTokens) {
					EntityNode node = new EntityNode(tk, e.getId(), allTokens, true/*isSource*/, Integer.parseInt(args[2]), null);
					output.collect(new Tuple2<String, EntityNode>(tk, node));
				}
				
			}
		});
			
			
		DataStream<Tuple2<String, EntityNode>> twitterTokens = entitiesFromTwitter.rebalance().flatMap(new FlatMapFunction<TwitterAPI, Tuple2<String, EntityNode>>() {
			@Override
			public void flatMap(TwitterAPI e, Collector<Tuple2<String, EntityNode>> output) throws Exception {
				Set<String> allTokens = e.getAllTokens(Boolean.parseBoolean(args[7]));
				
				for (String tk : allTokens) {
					EntityNode node = new EntityNode(tk, e.getId(), allTokens, false/*isTarget*/, Integer.parseInt(args[2]), null);
					node.setText(e.getText().isEmpty()? e.getFullText() : e.getText());
					output.collect(new Tuple2<String, EntityNode>(tk, node));
				}
				
			}
			
		});
		
		
		//Applies the token as a key.
		WindowedStream<Tuple2<String, EntityNode>, Tuple, TimeWindow> tokenMatchesWindow = 
				matchesTokens.rebalance().keyBy(0).timeWindow(Time.minutes(Integer.parseInt(args[3])), Time.minutes(1));//define the window FIX TO RECEIVE AS PARAMETER
		
		
		
		SingleOutputStreamOperator<Tuple2<String, EntityNode>> matchesWindowed = tokenMatchesWindow.apply(new WindowFunction<Tuple2<String,EntityNode>, Tuple2<String,EntityNode>, Tuple, TimeWindow>() {

			@Override
			public void apply(Tuple arg0, TimeWindow arg1, Iterable<Tuple2<String, EntityNode>> entities,
					Collector<Tuple2<String, EntityNode>> collector) throws Exception {
				for (Tuple2<String, EntityNode> ent : entities) {
					collector.collect(ent);
				}
				
			}
		});
		
		
		
		DataStream<BlockStructure> cluster = matchesWindowed.coGroup(twitterTokens).where(new KeySelector<Tuple2<String,EntityNode>, String>() {

			@Override
			public String getKey(Tuple2<String, EntityNode> ent) throws Exception {
				return ent.f0;
			}
		}).equalTo(new KeySelector<Tuple2<String,EntityNode>, String>() {
			
			@Override
			public String getKey(Tuple2<String, EntityNode> ent) throws Exception {
				return ent.f0;
			}
		}).window(SlidingProcessingTimeWindows.of(Time.minutes(Integer.parseInt(args[3])), Time.minutes(1))).apply(new CoGroupFunction<Tuple2<String,EntityNode>, Tuple2<String,EntityNode>, BlockStructure>() {

			@Override
			public void coGroup(Iterable<Tuple2<String, EntityNode>> entitiesFromMatches, Iterable<Tuple2<String, EntityNode>> entitiesFromTwitter,
					Collector<BlockStructure> collector) throws Exception {
				if (entitiesFromMatches.iterator().hasNext()) {
					BlockStructure block = new BlockStructure();
					for (Tuple2<String, EntityNode> match : entitiesFromMatches) {
						block.setId(match.f0);
						block.addInSource(match.f1);
					}
					
					for (Tuple2<String, EntityNode> twitter : entitiesFromTwitter) {
						block.addInTarget(twitter.f1);
					}
					collector.collect(block);
				}
				
			}
		});
		
//		cluster.print();
		
		
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
		
		
		SingleOutputStreamOperator<Tuple2<String, EntityNode>> entitySimilarities = cluster.rebalance().flatMap(new FlatMapFunction<BlockStructure, Tuple2<String, EntityNode>>() {

			@Override
			public void flatMap(BlockStructure block, Collector<Tuple2<String, EntityNode>> output) throws Exception {
				for (EntityNode eSource : block.getFromSource()) {
					for (EntityNode eTarget : block.getFromTarget()) {
						double sim = calculateSimilarity(block.getId(), eSource.getBlocks(), eTarget.getBlocks());
						if (sim >= 0) {
							eSource.addNeighbor(new TupleSimilarity2(eTarget.getIdAsLong(), sim, eTarget.getText()));
						}
					}
					
					output.collect(new Tuple2<String, EntityNode>(eSource.getIdAsString(), eSource));
				}
				
			}
			
			private double calculateSimilarity(String blockKey, Set<String> ent1, Set<String> ent2) {
//				int maxSize = Math.max(ent1.size() - 1, ent2.size() - 1);
				int maxSize = ent1.size() - 1;//the similarity will be based on the tokens of new api
				Set<String> intersect = new HashSet<String>(ent1);
				intersect.retainAll(ent2);


				// MACOBI strategy
//				if (!Collections.min(intersect).equals(blockKey)) {
//					return -1;
//				}

				if (maxSize > 0) {
					double x = (double) intersect.size() / maxSize;
					return x;
				} else {
					return 0;
				}
			}
			
//			private double calculateSimilarity(Integer blockKey, Set<Integer> ent1, Set<Integer> ent2) {
//				int maxSize = Math.max(ent1.size() - 1, ent2.size() - 1);
//				Set<Integer> intersect = new HashSet<Integer>(ent1);
//				intersect.retainAll(ent2);
//
//				// MACOBI strategy
//				if (!Collections.min(intersect).equals(blockKey)) {
//					return -1;
//				}
//
//				if (maxSize > 0) {
//					double x = (double) intersect.size() / maxSize;
//					return x;
//				} else {
//					return 0;
//				}
//			}
		});
		
		
		
		WindowedStream<Tuple2<String, EntityNode>, Tuple, TimeWindow> entityNeighborhood = 
				entitySimilarities.keyBy(0).timeWindow(Time.minutes(Integer.parseInt(args[3])), Time.minutes(Integer.parseInt(args[4])));
		
		
		SingleOutputStreamOperator<EntityNode> graph = entityNeighborhood.aggregate(new AggregateFunction<Tuple2<String,EntityNode>, EntityNode, EntityNode>() {

			@Override
			public EntityNode add(Tuple2<String, EntityNode> entity, EntityNode acc) {
				EntityNode newEntity = new EntityNode();
				newEntity.setId(entity.f1.getIdAsString());
				newEntity.setMaxNumberOfNeighbors(Integer.parseInt(args[2]));
				newEntity.addAllNeighbors(entity.f1.getNeighbors());
				newEntity.addAllNeighbors(acc.getNeighbors());
				return newEntity;
			}

			@Override
			public EntityNode createAccumulator() {
				return new EntityNode();
			}

			@Override
			public EntityNode getResult(EntityNode acc) {
				return acc;
			}

			@Override
			public EntityNode merge(EntityNode e1, EntityNode e2) {
				EntityNode newEntity = new EntityNode();
				newEntity.setId(e1.getIdAsString());
				newEntity.setMaxNumberOfNeighbors(Integer.parseInt(args[2]));
				newEntity.addAllNeighbors(e1.getNeighbors());
				newEntity.addAllNeighbors(e2.getNeighbors());
				return newEntity;
			}
		});
		
		SimpleDateFormat sdf = new SimpleDateFormat("HH;mm;ss");
		
		//Execute a pruning of the neighbors
		DataStreamSink<String> output = graph.rebalance().map(new MapFunction<EntityNode, String>() {
			@Override
			public String map(EntityNode node) throws Exception {
//				if (node.getId() == 30) {
//					System.out.println();
//				}
//				node.pruningWNP();
				return node.getIdAsString() + ">" + node.toString();
			}
		}).writeAsText(args[5] + args[2] + "-" + args[3] + "-" + args[4] + "-" + args[7] + "-"+ sdf.format(Calendar.getInstance().getTime()) + ".txt");
		
		
		env.execute("PRIME");
	}
	
	public void stop() {
		System.exit(0);
	}
}
