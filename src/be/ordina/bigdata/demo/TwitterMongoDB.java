package be.ordina.bigdata.demo;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Scanner;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.UserMentionEntity;
import twitter4j.conf.ConfigurationBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class TwitterMongoDB {
	
	private static ConfigurationBuilder cb;  
	private DB db;  
	private DBCollection items;  
	  
	 /**  
	   * static block used to construct a connection with tweeter   
	   * with twitter4j configuration with provided settings.   
	   */  
	  static {  
	    cb = new ConfigurationBuilder();  
	    cb.setDebugEnabled(true);  
	    cb.setOAuthConsumerKey("1HrWSchAql1Byhqk3izXPjLSa");  
	    cb.setOAuthConsumerSecret("MTObW6KaWP1rHnaQIiO4d09T3BvSOJ8IQY03cK2ZhxCjQLx9Ou");  
	    cb.setOAuthAccessToken("474313291-vXas8qby0TFRs4wm1ryZ3Uzxvt3c1na0KZsvhnUo");  
	    cb.setOAuthAccessTokenSecret("5CSvOUUltpsg6KyuBnI6SnDiPNNfTu4yML1OAaP64VVgf");  
	  }    
	  
	  public TwitterMongoDB() {  
	    try {  
	      // on constructor load initialize MongoDB and load collection  
	      initMongoDB();  
	      items = db.getCollection("tweets");  
	    } catch (MongoException ex) {  
	      System.out.println("MongoException :" + ex.getMessage());  
	    }  
	  }  
	  
	  /**  
	   * initMongoDB been called in constructor so every object creation this  
	   * initialize MongoDB.  
	   */  
	  private void initMongoDB() throws MongoException {  
	    try {  
	      db = new Mongo("127.0.0.1").getDB("tweetTestDB");  
	    } catch (UnknownHostException ex) {  
	      System.out.println("MongoDB Connection Error :" + ex.getMessage());  
	    }  
	  }
	  

	  public static void main(String[] args) {  
		  TwitterMongoDB taskObj = new TwitterMongoDB();  
		  taskObj.loadMenu();  
	  }  
	  
	  private void loadMenu() {  
	     System.out.println("================\n\tTwitter Task\n===============");  
	     System.out.println("1 - Load 100 Tweets & save into Mongo DB");  
	     System.out.println("2 - Load Top 5 Retweet");  
	     System.out.println("5 - Exit");  
	     System.out.print("Please enter your selection:\t");  
	     Scanner scanner = new Scanner(System.in);  
	     int selection = scanner.nextInt();  
	     if (selection == 1) {  
	       getTweetByQuery(true);  
	     } else if (selection == 2) {  
	       getTopRetweet();  
	     } else if (selection == 5) {  
	       System.exit(0);  
	     } else {  
	       System.out.println("Wrong Selection Found..\n\n");  
	       loadMenu();  
	     }  
	   }  
	  
	 
	  /**  
	   * void getTweetByQuery method used to fetch records from twitter.com using  
	   * Query class to define query for search param with record count.  
	   * QueryResult persist result from twitter and provide into the list to  
	   * iterate records 1 by one and later on item.insert is call to store this  
	   * BasicDBObject into MongoDB items Collection.  
	   *  
	   * @param url an absolute URL giving the base location of the image  
	   * @see BasicDBObject, DBCursor, TwitterFactory, Twitter  
	   */  
	  private void getTweetByQuery(boolean loadRecords) {  
	    if (cb != null) {  
	      TwitterFactory tf = new TwitterFactory(cb.build());  
	      Twitter twitter = tf.getInstance();  
	      try {  
	        Query query = new Query("mongoDB");  
	        query.setCount(50);  
	        QueryResult result = twitter.search(query);  
	        
	        System.out.println("Getting Tweets...");  
	        
	        List<Status> tweets = result.getTweets();  
	        for (Status tweet : tweets) {  
	          BasicDBObject basicObj = new BasicDBObject();  
	          basicObj.put("user_name", tweet.getUser().getScreenName());  
	          basicObj.put("retweet_count", tweet.getRetweetCount());  
	          basicObj.put("tweet_followers_count",  
	              tweet.getUser().getFollowersCount());  
	          UserMentionEntity[] mentioned = tweet.getUserMentionEntities();  
	          basicObj.put("tweet_mentioned_count", mentioned.length);  
	          basicObj.put("tweet_ID", tweet.getId());  
	          basicObj.put("tweet_text", tweet.getText());  
	          
	          try {  
	            items.insert(basicObj);  
	          } catch (Exception e) {  
	            System.out.println("MongoDB Connection Error : "  
	                + e.getMessage());  
	            loadMenu();  
	          }  
	        }  
	        // Printing fetched records from DB.  
	        if (loadRecords) {  
	          getTweetsRecords();  
	        }  
	      } catch (TwitterException te) {  
	        te.printStackTrace();
	        if (te.getStatusCode() == 401) {  
	          System.out.println("Twitter Error : \nAuthentication "  
	              + "credentials (https://dev.twitter.com/pages/auth) "  
	              + "were missing or incorrect.\nEnsure that you have "  
	              + "set valid consumer key/secret, access "  
	              + "token/secret, and the system clock is in sync.");  
	        } else {  
	          System.out.println("Twitter Error : " + te.getMessage());  
	        }  
	        loadMenu();  
	      }  
	    } else {  
	      System.out.println("MongoDB is not Connected!");  
	    }  
	  }  
	  
	  /**
	   * retrieve tweets from db
	   */
	  private void getTweetsRecords() {
	      BasicDBObject fields = new BasicDBObject("_id", true).append("user_name", true).append("tweet_text", true);
	      DBCursor cursor = items.find(new BasicDBObject(), fields);

	      while (cursor.hasNext()) {
	          System.out.println(cursor.next());
	      }
	      loadMenu();
	  }
	  
	  /**
	   * void method print fetched top retweet records from preloaded items
	   * collection with the help of BasicDBObject class defined sort with desc
	   * with fixed limit 10.
	   *
	   * @see BasicDBObject, DBCursor
	   */
	  private void getTopRetweet() {
	      if (items.count() <= 0) {
	          getTweetByQuery(false);
	      }

	      BasicDBObject query = new BasicDBObject();
	      query.put("retweet_count", -1);
	      DBCursor cursor = items.find().sort(query).limit(10);
	      System.out.println("items length " + items.count());
	      while (cursor.hasNext()) {
	          System.out.println(cursor.next());
	      }
	      loadMenu();
	  }

	  
	  /*
		 * db.tweets.ensureIndex({tweet_text: "text"})
		 * 
		 * db.tweets.aggregate([{$match:{$text:{$search:"mongodb"}}}
		 	,{$group:{_id:"$user_name", count:{$sum:1}}}]);
		 * 
		 * 1. match on the text index to find the tweets containing the searched word
		 * 2. group on the festival field and do a count of the tweets
		 * 3. sort so highest count is on top
		 * 
		 *  
		 *> db.toFollow.update({},{'$unset':{"maxId":true}},{'multi':true});
		 *> db.toFollow.update({},{'$unset':{"sinceId":true}},{'multi':true});
		 *> db.tweetColl.remove({});
		 */
	  
	  
}
