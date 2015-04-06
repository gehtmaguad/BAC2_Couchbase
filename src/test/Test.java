package test;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class Test {

	public static int numberOfUsers = 1000;
	public static int numberOfBlogs = 1000;
	public static int numberOfComments = 1000;
	public static int numberOfLikes = 1000;

	public static void main(String[] args) {

		// Connection Details
		Cluster cluster = CouchbaseCluster.create("192.168.122.120");

		executeInsertEmbeddedWithNewUser(cluster);
		// executeInsertReferenced(cluster);

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

	public static void executeInsertEmbeddedWithExistingUser(Cluster cluster) {

		// Helper Variable
		int count;

		// Get Buckets
		Bucket userBucket = cluster.openBucket("embedded_User", "");
		Bucket blogBucket = cluster.openBucket("embedded_Blog", "");

		// Insert User
		for (count = 0; count < numberOfUsers; count++) {
			JsonObject user = JsonObject
					.empty()
					.put("id", count + 1)
					.put("vorname", randomText(100))
					.put("nachname", randomText(150))
					.put("email",
							randomText(120) + "@" + randomText(20) + "."
									+ randomText(10));
			JsonDocument doc = JsonDocument.create(String.valueOf(count + 1),
					user);
			userBucket.upsert(doc);
		}

		// Insert Comment
		int b;
		int c;
		int l;

		// Get User and then Insert
		for (b = 1; b <= numberOfBlogs; b++) {

			Map<String, String> user = new HashMap<String, String>();
			user = selectUserById(randomNumber(1, numberOfUsers));

			JsonObject blog = JsonObject.empty().put("id", b)
					.put("blogpost", randomText(5000))
					.put("user_id", user.get("user_id"))
					.put("vorname", user.get("vorname"))
					.put("nachname", user.get("nachname"))
					.put("email", user.get("email"));

			ArrayList<JsonObject> commentList = new ArrayList<JsonObject>();

			for (c = 1; c <= 3; c++) {

				user = selectUserById(randomNumber(1, numberOfUsers));

				JsonObject comment = JsonObject.empty().put("id", c)
						.put("comment", randomText(2000))
						.put("user_id", user.get("user_id"))
						.put("vorname", user.get("vorname"))
						.put("nachname", user.get("nachname"))
						.put("email", user.get("email"));

				ArrayList<JsonObject> likeList = new ArrayList<JsonObject>();

				for (l = 1; l <= 3; l++) {

					user = selectUserById(randomNumber(1, numberOfUsers));

					JsonObject like = JsonObject.empty().put("id", l)
							.put("user_id", user.get("user_id"))
							.put("vorname", user.get("vorname"))
							.put("nachname", user.get("nachname"))
							.put("email", user.get("email"));

					likeList.add(like);
				}

				comment.put("Likes", likeList);
				commentList.add(comment);

			}

			blog.put("Comment", commentList);

			JsonDocument doc = JsonDocument.create(String.valueOf(b), blog);
			blogBucket.upsert(doc);
		}		

	}

	public static void executeInsertEmbeddedWithNewUser(Cluster cluster) {

		// Helper Variable
		int count;

		// Get Buckets
		Bucket userBucket = cluster.openBucket("embedded_User", "");
		Bucket blogBucket = cluster.openBucket("embedded_Blog", "");

		// Insert User
		for (count = 0; count < numberOfUsers; count++) {
			JsonObject user = JsonObject
					.empty()
					.put("id", count + 1)
					.put("vorname", randomText(100))
					.put("nachname", randomText(150))
					.put("email",
							randomText(120) + "@" + randomText(20) + "."
									+ randomText(10));
			JsonDocument doc = JsonDocument.create(String.valueOf(count + 1),
					user);
			userBucket.upsert(doc);
		}

		// Insert Comment
		int b;
		int c;
		int l;
		
		// Create User and Insert
		for (b = 1; b <= numberOfBlogs; b++) {

			JsonObject blog = JsonObject
					.empty()
					.put("id", b)
					.put("blogpost", randomText(5000))
					.put("vorname", randomText(100))
					.put("nachname", randomText(150))
					.put("email",
							randomText(120) + "@" + randomText(20) + "."
									+ randomText(10));

			ArrayList<JsonObject> commentList = new ArrayList<JsonObject>();

			for (c = 1; c <= 3; c++) {

				JsonObject comment = JsonObject
						.empty()
						.put("id", c)
						.put("comment", randomText(2000))
						.put("vorname", randomText(100))
						.put("nachname", randomText(150))
						.put("email",
								randomText(120) + "@" + randomText(20) + "."
										+ randomText(10));

				ArrayList<JsonObject> likeList = new ArrayList<JsonObject>();

				for (l = 1; l <= 3; l++) {

					JsonObject like = JsonObject
							.empty()
							.put("id", l)
							.put("vorname", randomText(100))
							.put("nachname", randomText(150))
							.put("email",
									randomText(120) + "@" + randomText(20)
											+ "." + randomText(10));

					likeList.add(like);
				}

				comment.put("Likes", likeList);
				commentList.add(comment);

			}

			blog.put("Comment", commentList);

			JsonDocument doc = JsonDocument.create(String.valueOf(b), blog);
			blogBucket.upsert(doc);
		}

	}
	
	public static void executeInsertReferenced(Cluster cluster) {

		// Helper Variable
		int count;

		// Get Buckets
		Bucket userBucket = cluster.openBucket("referenced_User", "");
		Bucket blogBucket = cluster.openBucket("referenced_Blog", "");
		Bucket commentBucket = cluster.openBucket("referenced_Comment", "");
		Bucket likeBucket = cluster.openBucket("referenced_Likes", "");

		// Insert User
		for (count = 0; count < numberOfUsers; count++) {
			JsonObject user = JsonObject
					.empty()
					.put("id", count + 1)
					.put("vorname", randomText(100))
					.put("nachname", randomText(150))
					.put("email",
							randomText(120) + "@" + randomText(20) + "."
									+ randomText(10));
			JsonDocument doc = JsonDocument.create(String.valueOf(count + 1),
					user);
			userBucket.upsert(doc);
		}

		// Insert Blog
		for (count = 0; count < numberOfBlogs; count++) {
			JsonObject blog = JsonObject.empty().put("id", count + 1)
					.put("blogpost", randomText(5000))
					.put("user_id", randomNumber(1, numberOfUsers));

			JsonDocument doc = JsonDocument.create(String.valueOf(count + 1),
					blog);
			blogBucket.upsert(doc);
		}

		// Insert Comment
		for (count = 0; count < numberOfComments; count++) {
			JsonObject comment = JsonObject.empty().put("id", count + 1)
					.put("comment", randomText(5000))
					.put("user_id", randomNumber(1, numberOfUsers))
					.put("blog_id", randomNumber(1, numberOfBlogs));

			JsonDocument doc = JsonDocument.create(String.valueOf(count + 1),
					comment);
			commentBucket.upsert(doc);
		}

		// Insert Likes
		for (count = 0; count < numberOfLikes; count++) {
			JsonObject like = JsonObject.empty().put("id", count + 1)
					.put("comment_id", randomNumber(1, numberOfComments))
					.put("user_id", randomNumber(1, numberOfUsers));

			JsonDocument doc = JsonDocument.create(String.valueOf(count + 1),
					like);
			likeBucket.upsert(doc);
		}

	}
	

	public static void selectN1ql() {
		DefaultHttpClient httpclient = new DefaultHttpClient();
		try {
			// specify the host, protocol, and port
			HttpHost target = new HttpHost("192.168.122.57", 8093, "http");

			// specify the get request
			HttpGet getRequest = new HttpGet(
					"/query?statement=SELECT%20rb.blogpost,%20ru.nachname%20FROM%20referenced_Blog%20rb%20JOIN%20referenced_User%20ru%20ON%20KEYS%20rb.user_id");

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
	

	public static Map<String, String> selectUserById(int id) {
		
		Map<String, String> resultSet = null;
		DefaultHttpClient httpclient = new DefaultHttpClient();
		
		try {
			// specify the host, protocol, and port
			HttpHost target = new HttpHost("192.168.122.57", 8093, "http");

			// specify the get request
			HttpGet getRequest = new HttpGet(
					"/query?statement=SELECT%20*%20FROM%20referenced_User%20WHERE%20id="
							+ id);

			System.out.println("executing request to " + target);

			HttpResponse httpResponse = httpclient.execute(target, getRequest);
			HttpEntity entity = httpResponse.getEntity();

			if (entity != null) {
				String retSrc = EntityUtils.toString(entity);
				// parsing JSON
				JSONObject result = new JSONObject(retSrc); // Convert String to
															// JSON Object

				JSONArray userList = result.getJSONArray("results");
				JSONObject user = userList.getJSONObject(0).getJSONObject(
						"referenced_User");

				// Parse Result Set
				resultSet = new HashMap<String, String>();
				resultSet.put("user_id", String.valueOf(user.getInt("id")));
				resultSet.put("vorname", user.getString("vorname"));
				resultSet.put("nachname", user.getString("nachname"));
				resultSet.put("email", user.getString("email"));
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// When HttpClient instance is no longer needed,
			// shut down the connection manager to ensure
			// immediate deallocation of all system resources
			httpclient.getConnectionManager().shutdown();
		}
		
		return resultSet;
	}

}