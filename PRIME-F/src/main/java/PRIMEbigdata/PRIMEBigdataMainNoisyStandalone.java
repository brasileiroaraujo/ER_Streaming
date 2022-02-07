package PRIMEbigdata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import DataStructures.BlockStructureGraph;
import DataStructures.EntityNodeGraph;
import DataStructures.EntityProfile;
import DataStructures.TupleSimilarity;

//localhost:9092 localhost:2181 20 200 20 outputs/
public class PRIMEBigdataMainNoisyStandalone {
	
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(Integer.parseInt(args[6]));
		
		boolean isNoisyScenario = Boolean.parseBoolean(args[7]);
		
		DataSet<String> source = env.readTextFile(args[0]);//readTextFile(args[0]);
		DataSet<String> target = env.readTextFile(args[1]);
		
//		DataStream<String> lines = env.addSource(new FlinkKafkaConsumer011<String>("mytopic", new SimpleStringSchema(), properties));
		
//		DataStream<EntityProfile> entities = lines.rebalance().map(s -> new EntityProfile(s, true));
		
		UnionOperator<String> entities = source.union(target);
		
		MapOperator<String, EntityProfile> nodes = entities.rebalance().map(s -> new EntityProfile(s, isNoisyScenario));
		
		//Receive the entities and extract the token from the attribute values.
		DataSet<Tuple2<Integer, EntityNodeGraph>> entitiesTokens = nodes.rebalance().flatMap(new FlatMapFunction<EntityProfile, Tuple2<Integer, EntityNodeGraph>>() {
			
			@Override
			public void flatMap(EntityProfile e, Collector<Tuple2<Integer, EntityNodeGraph>> output) throws Exception {
				for (Integer tk : e.getSetOfTokens()) {
//					if ((e.getKey() == 7925 && e.isSource()) || (e.getKey() == 14889 && !e.isSource())) {
//						System.out.println(e.getKey() + " - " + e.getSetOfTokens());
//					}
					EntityNodeGraph node = new EntityNodeGraph(tk, e.getKey(), e.getSetOfTokens(), e.isSource(), Integer.parseInt(args[2]), e.getIncrementID());
					output.collect(new Tuple2<Integer, EntityNodeGraph>(tk, node));
				}
				
			}
			
			
			
		});
		
		
		//Applies the token as a key.
		UnsortedGrouping<Tuple2<Integer, EntityNodeGraph>> tokenKeys = 
				entitiesTokens.rebalance().groupBy(0);//.timeWindow(Time.seconds(Integer.parseInt(args[3])), Time.seconds(Integer.parseInt(args[4])));//define the window
		

		
		//Group the entities with the same token (blocking using the token as a key).
		//<Iterable<Tuple2<Integer,EntityNodeGraph>>, Collector<Tuple2<Integer,EntityNodeGraph>>>
		GroupReduceOperator<Tuple2<Integer, EntityNodeGraph>, Tuple2<Integer, EntityNodeGraph>> tokenBlocks = tokenKeys.reduceGroup(new GroupReduceFunction<Tuple2<Integer,EntityNodeGraph>, Tuple2<Integer, EntityNodeGraph>>() {

			@Override
			public void reduce(Iterable<Tuple2<Integer, EntityNodeGraph>> values, Collector<Tuple2<Integer, EntityNodeGraph>> out) throws Exception {
				Set<EntityNodeGraph> source = new HashSet<EntityNodeGraph>();
				Set<EntityNodeGraph> target = new HashSet<EntityNodeGraph>();
				Integer blockKey = null;
				for (Tuple2<Integer, EntityNodeGraph> tuple : values) {
					if (blockKey == null) {
						blockKey = tuple.f0;
					}
					
					if (tuple.f1.isSource()) {
						source.add(tuple.f1);
					} else {
						target.add(tuple.f1);
					}
				}
				
				//filter
//				if (source.size() < 20000 && target.size() < 20000) {
					if (source.size() > 0 && target.size() > 0) {
						System.out.println(blockKey);
						System.out.println(source.size());
						System.out.println(target.size());
					}
					
					for (EntityNodeGraph sEnt : source) {
						for (EntityNodeGraph tEnt : target) {
							double sim = calculateSimilarity(blockKey, sEnt.getBlocks(), tEnt.getBlocks());
							if (sim >= 0) {
								sEnt.addNeighbor(new TupleSimilarity(tEnt.getId(), sim));
							}
						}
						if (!target.isEmpty() && !sEnt.getNeighbors().isEmpty()) {
							out.collect(new Tuple2<Integer, EntityNodeGraph>(sEnt.getId(), sEnt));
						}
						
					}
//				}
				
			}
			
			private double calculateSimilarity(Integer blockKey, Set<Integer> ent1, Set<Integer> ent2) {
				int maxSize = ent1.size() - 1;//Math.min(ent1.size() - 1, ent2.size() - 1);
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
		
		
		ReduceOperator<Tuple2<Integer, EntityNodeGraph>> graph = tokenBlocks.groupBy(0).reduce(new ReduceFunction<Tuple2<Integer,EntityNodeGraph>>() {
			
			@Override
			public Tuple2<Integer, EntityNodeGraph> reduce(Tuple2<Integer, EntityNodeGraph> value1,
					Tuple2<Integer, EntityNodeGraph> value2) throws Exception {
				EntityNodeGraph newEntity = new EntityNodeGraph();
				newEntity.setId(value1.f1.getId());
				newEntity.setMaxNumberOfNeighbors(Integer.parseInt(args[2]));
				newEntity.addAllNeighbors(value1.f1.getNeighbors());
				newEntity.addAllNeighbors(value2.f1.getNeighbors());
				return new Tuple2<Integer, EntityNodeGraph>(value1.f0, newEntity);
			}
		});
				
		
		
		//Execute a pruning of the neighbors
		DataSink<String> output = graph.rebalance().map(new MapFunction<Tuple2<Integer, EntityNodeGraph>, String>() {
			@Override
			public String map(Tuple2<Integer, EntityNodeGraph> node) throws Exception {
				node.f1.pruningWNP();
				return node.f1.getId() + ">" + node.f1.toString();
			}
		}).writeAsText(args[5]);
		
		env.execute();
	}
}

