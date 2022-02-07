//package TwitterApi;
//
//import java.text.ParseException;
//import java.util.List;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingQueue;
//
//import org.json.JSONObject;
//
//import com.google.common.collect.Lists;
//import com.twitter.hbc.ClientBuilder;
//import com.twitter.hbc.core.Client;
//import com.twitter.hbc.core.Constants;
//import com.twitter.hbc.core.Hosts;
//import com.twitter.hbc.core.HttpHosts;
//import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
//import com.twitter.hbc.core.event.Event;
//import com.twitter.hbc.core.processor.StringDelimitedProcessor;
//import com.twitter.hbc.httpclient.auth.Authentication;
//import com.twitter.hbc.httpclient.auth.OAuth1;
//
//import FootballApi.JsonManager;
//
//public class Teste {
//
//	public static void main(String[] args) throws InterruptedException, ParseException {
//		String consumer_key = "jrVA0gkCeKtmPsmFOnyJCC8wz"; //input('Consumer Key: ') #jrVA0gkCeKtmPsmFOnyJCC8wz
//	    String consumer_secret = "maEHcUInH767Ot4fZmmIpWVx8rWTUzD01NaXOQJtn4hYfJtMkv"; //#getpass('Consumer Secret: ') #maEHcUInH767Ot4fZmmIpWVx8rWTUzD01NaXOQJtn4hYfJtMkv
//	    String access_token = "1154435537765355521-0e1UOx64RigNtsp5SyKTx7it6A2own"; //#input('Access Token: ') #1154435537765355521-0e1UOx64RigNtsp5SyKTx7it6A2own
//	    String access_token_secret = "l2OyQdhzLWcxG8pZuYZiSbtnpuxmKRQBuHxqXjq6Yhj4J"; //#getpass('Access Token Secret: ') #l2OyQdhzLWcxG8pZuYZiSbtnpuxmKRQBuHxqXjq6Yhj4J
//			    
//			    
//		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
//		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000000);
//		BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(10000);
//
//		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
//		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
//		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
//		// Optional: set up some followings and track terms
//		List<Long> followings = Lists.newArrayList(1234L, 566788L);
//		List<String> terms = Lists.newArrayList("twitter", "api");
//		hosebirdEndpoint.followings(followings);
//		hosebirdEndpoint.trackTerms(terms);
//
//		// These secrets should be read from a config file
//		Authentication hosebirdAuth = new OAuth1(consumer_key, consumer_secret, access_token, access_token_secret);
//		
//		ClientBuilder builder = new ClientBuilder()
//		  .name("Hosebird-Client-01")                              // optional: mainly for the logs
//		  .hosts(hosebirdHosts)
//		  .authentication(hosebirdAuth)
//		  .endpoint(hosebirdEndpoint)
//		  .processor(new StringDelimitedProcessor(msgQueue))
//		  .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events
//
//		Client hosebirdClient = builder.build();
//		// Attempts to establish a connection.
//		hosebirdClient.connect();
//		
//		while (!hosebirdClient.isDone()) {
//			JSONObject json = JsonManager.parserToJson(msgQueue.take());
//			System.out.println(json);
//		}
//	}
//
//}
