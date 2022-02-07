//package PRIMEbigdata;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashSet;
//import java.util.Properties;
//import java.util.Set;
//
//import org.apache.flink.api.common.JobExecutionResult;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSink;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.WindowedStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//import org.apache.flink.util.Collector;
//
//import DataStructures.Attribute;
//import DataStructures.Cluster;
//import DataStructures.EntityProfile;
//import DataStructures.Node;
//import exceptions.EndException;
//import scala.Tuple2;
//import tokens.KeywordGenerator;
//import tokens.KeywordGeneratorImpl;
//
//public class PRIMEBigdataRefined {
//	public static void main(String[] args) throws Exception {
//		//INIT TIME
//		long initTime = System.currentTimeMillis();
////		try {		
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		Properties properties = new Properties();
//		properties.setProperty("bootstrap.servers", args[0]);
//		// only required for Kafka 0.8
//		properties.setProperty("zookeeper.connect", args[1]);
//		properties.setProperty("group.id", "test");
//		
//		DataStream<String> lines = env.addSource(new FlinkKafkaConsumer("mytopic", new SimpleStringSchema(), properties));
//		
//		initTime = System.currentTimeMillis();
//		
//		// the rebelance call is causing a repartitioning of the data so that all machines
//		DataStream<EntityProfile> entities = lines.rebalance().map(new MapFunction<String, EntityProfile>() {
//
//			@Override
//			public EntityProfile map(String s) throws EndException {
////					if (s.equals("THEEND")) {
////						throw new EndException();
////					}
//				return new EntityProfile(s);
//			}
//		});
//		
//		DataStream<Cluster> streamOfPairs = entities.rebalance().flatMap(new FlatMapFunction<EntityProfile, Cluster>() {
//
//			@Override
//			public void flatMap(EntityProfile se, Collector<Cluster> output) throws Exception {
//				Set<Integer> cleanTokens = new HashSet<Integer>();
//
//				for (Attribute att : se.getAttributes()) {
//					KeywordGenerator kw = new KeywordGeneratorImpl();
//					for (String string : kw.generateKeyWords(att.getValue())) {
//						cleanTokens.add(string.hashCode());
//					}
//				}
//
//				for (Integer tk : cleanTokens) {
//					Cluster cluster = new Cluster(tk, new HashSet<Node>(), new HashSet<Node>());
//					cluster.addInNewCollection(new Node(tk, se.getKey(), cleanTokens, new HashSet<>(), se.isSource(), false));
//					output.collect(cluster);
//				}
//			}
//		});
//
//		
//		KeyedStream<Cluster, Integer> entityBlocks = streamOfPairs.rebalance().keyBy(new KeySelector<Cluster, Integer>() {
//			@Override
//			public Integer getKey(Cluster cluster) throws Exception {
//				return cluster.getTokenkey();
//			}
//		});//.timeWindow(Time.seconds(Integer.parseInt(args[2])), Time.seconds(Integer.parseInt(args[3])));//define the window
//		
//		
//		SingleOutputStreamOperator<Cluster> entityClusters = entityBlocks.reduce(new ReduceFunction<Cluster>() {
//			
//			@Override
//			public Cluster reduce(Cluster c1, Cluster c2) throws Exception {
//				c1.addAllNewCollection(c2.getNewCollection());
//				c1.addAllCollection(c2.getCollection());
//				return new Cluster(c1.getTokenkey(), c1.getCollection(), c1.getNewCollection());
//			}
//			
//		});
//		
//		WindowedStream<Cluster, Integer, TimeWindow> storedClusters = entityClusters.rebalance().keyBy(new KeySelector<Cluster, Integer>() {
//			@Override
//			public Integer getKey(Cluster cluster) throws Exception {
//				cluster.mergeCollections();
//				return cluster.getTokenkey();
//			}
//		}).window(SlidingProcessingTimeWindows.of(Time.seconds(Integer.parseInt(args[2])), Time.seconds(Integer.parseInt(args[3]))));//define the window
//		
//		
//		SingleOutputStreamOperator<Cluster> reducedClusters = storedClusters.reduce(new ReduceFunction<Cluster>() {
//			@Override
//			public Cluster reduce(Cluster c1, Cluster c2) throws Exception {
//				c1.addAllNewCollection(c2.getNewCollection());
//				c1.addAllCollection(c2.getCollection());
//				return new Cluster(c1.getTokenkey(), c1.getCollection(), c1.getNewCollection());
//			}
//		});
//		
//		SingleOutputStreamOperator<Node> pairEntityBlock = entityClusters.rebalance().flatMap(new FlatMapFunction<Cluster, Node>() {
//
//			@Override
//			public void flatMap(Cluster value, Collector<Node> out) throws Exception {
//				ArrayList<Node> entitiesToCompare = new ArrayList<Node>(value.getCollection());
//				ArrayList<Node> comparedEntites = new ArrayList<Node>();
//				
//				for (int i = 0; i < entitiesToCompare.size(); i++) {
//					Node n1 = entitiesToCompare.get(i);
//					for (int j = i+1; j < entitiesToCompare.size(); j++) {
//						Node n2 = entitiesToCompare.get(j);
//						//Only compare nodes from distinct sources and marked as new (avoid recompute comparisons)
//						if (n1.isSource() != n2.isSource() && (n1.isMarked() || n2.isMarked())) {
//							double similarity = calculateSimilarity(n1.getToken(), n1.getBlocks(), n2.getBlocks());
//							if (similarity >= 0) {
//								if (n1.isSource()) {
//									n1.addNeighbor(new Tuple2<Integer, Double>(n2.getId(), similarity));
//									comparedEntites.add(n1);
//								} else {
//									n2.addNeighbor(new Tuple2<Integer, Double>(n1.getId(), similarity));
//									comparedEntites.add(n2);
//								}
//							}
//						}
//					}
//				}
//				
//				for (Node node : comparedEntites) {
//					node.setMarked(false);
//					out.collect(node);
//				}
//				
//			}
//			
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
//		});
//		
//		
//		KeyedStream<Node, Integer> nodeKeyed = pairEntityBlock.keyBy(new KeySelector<Node, Integer>() {
//			@Override
//			public Integer getKey(Node node) throws Exception {
//				return node.getId();
//			}
//		});
//		
//		SingleOutputStreamOperator<Node> nodesGrouped = nodeKeyed.reduce(new ReduceFunction<Node>() {
//
//			@Override
//			public Node reduce(Node n1, Node n2) throws Exception {
//				n1.addAllNeighbor(n2);
//				return n1;
//			}
//		});
//				
//		
//		SingleOutputStreamOperator<String> prunedGraph = nodesGrouped.rebalance().map(new MapFunction<Node, String>() {
//
//			@Override
//			public String map(Node node) throws Exception {
//				node.pruning();
//				return node.getId() + ">" + node.toString();
//			}
//		});
//		
//		DataStreamSink<String> prunedBlocks = prunedGraph.rebalance().filter(new FilterFunction<String>() {
//			
//			@Override
//			public boolean filter(String value) throws Exception {
//				if (value.split(">").length > 1) {
//					return true;
//				}
//				return false;
//			}
//			
//		}).writeAsText(args[4]);
//		
//		
//		JobExecutionResult result = env.execute("PRIMEBigdataRefined");
//			
////		} catch (Exception e) {
////			if (e.getLocalizedMessage().equals("exceptions.EndException")) {
////				System.out.println("----------------------------END------------------------------");
////				System.out.println("Execution time: " + ((System.currentTimeMillis() - initTime)/1000) + " sec.");
////			} else {
////				e.printStackTrace();
////			}
////			
////		}
//	}
//}
