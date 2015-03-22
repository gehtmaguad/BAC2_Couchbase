package test;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Random;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class Test {
	 
	  public static void main(String[] args) {
		  
		  Cluster cluster = CouchbaseCluster.create("192.168.122.120");
		  
		  executeInsertEmbedded(cluster);  
		  
		  cluster.disconnect();
	  }

		public static String randomText(Integer bits) {

			SecureRandom random = new SecureRandom();
			String randomString = new BigInteger(bits, random).toString(32);
			return randomString;

		}

		public static int randomNumber(Integer min, Integer max) {

			Random rand = new Random();
			int randomNum = rand.nextInt((max - min) + 1) + min;
			return randomNum;
		}

	  
	  public static void executeInsertEmbedded(Cluster cluster) {
		  
			// Helper Variable
			int count;
			int numberOfUsers = 1000;
			int numberOfBlogs = 1000;
			int numberOfComments = 1000;
			int numberOfLikes = 1000;
			
			// Get Buckets
			Bucket bucket = cluster.openBucket("embedded", "");
			
			// Insert User
			for (count = 0; count < numberOfUsers; count++) {
				JsonObject user = JsonObject.empty()
						.put("id", count + 1)
						.put("vorname", randomText(100) )
						.put("nachname", randomText(150) )
						.put("email", randomText(120) + "@" + randomText(20) + "."
						+ randomText(10));
				JsonDocument doc = JsonDocument.create(String.valueOf(count), user);
				bucket.upsert(doc);
			}
		  
	  }
	  
	  public static void selectN1ql() {
	    DefaultHttpClient httpclient = new DefaultHttpClient();
	    try {
	      // specify the host, protocol, and port
	      HttpHost target = new HttpHost("192.168.122.57", 8093, "http");
	       
	      // specify the get request
	      HttpGet getRequest = new HttpGet("/query?statement=SELECT%20rb.blogpost,%20ru.nachname%20FROM%20referenced_Blog%20rb%20JOIN%20referenced_User%20ru%20ON%20KEYS%20rb.user_id");
	      
	 
	      System.out.println("executing request to " + target);
	 
	      HttpResponse httpResponse = httpclient.execute(target, getRequest);
	      HttpEntity entity = httpResponse.getEntity();
	 
	      System.out.println("----------------------------------------");
	      System.out.println(httpResponse.getStatusLine());
	      Header[] headers = httpResponse.getAllHeaders();
	      for (int i = 0; i < headers.length; i++) {
	        System.out.println(headers[i]);
	      }
	      System.out.println("----------------------------------------");
	 
	      if (entity != null) {
	        System.out.println(EntityUtils.toString(entity));
	      }
	 
	    } catch (Exception e) {
	      e.printStackTrace();
	    } finally {
	      // When HttpClient instance is no longer needed,
	      // shut down the connection manager to ensure
	      // immediate deallocation of all system resources
	      httpclient.getConnectionManager().shutdown();
	    }
	  }
	}