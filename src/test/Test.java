package test;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
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

	public static int numberOfUsers = 100000;
	public static int numberOfBlogs = 100000;
	public static int numberOfComments = 100000;
	public static int numberOfLikes = 100000;

	public static void main(String[] args) {

		Cluster cluster = CouchbaseCluster.create("192.168.122.120");
		String n1qlIp = "192.168.122.57";

		// executeInsertReferenced(cluster);

		// Tag Top Blogger
		// long startTime = System.nanoTime();
		// HashMap<String, String> result = tagTopBloggerReferencedN1QL(n1qlIp);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// Loop through Referenced Documents
		// long startTime = System.nanoTime();
		// ArrayList<HashMap<String, String>> result =
		// selectBlogWithAssociatesReferencedN1QL(n1qlIp);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// Get one merged Document from Referenced Documents
		// long startTime = System.nanoTime();
		// HashMap<String, String> result =
		// selectBlogWithAssociatesReferencedN1QLSingle(n1qlIp, 252);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// executeInsertEmbeddedWithNewUser(cluster);

		// Tag Top Blogger
		// long startTime = System.nanoTime();
		// HashMap<String, String> result = tagTopBloggerEmbeddedN1QL(n1qlIp);;
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// Loop through Embedded Documents
		// long startTime = System.nanoTime();
		// ArrayList<HashMap<String, String>> result =
		// selectBlogWithAssociatesEmbeddedN1QL(n1qlIp);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// // Get one Document from Embedded Documents
		// long startTime = System.nanoTime();
		// HashMap<String, String> result =
		// selectBlogWithAssociatesEmbeddedN1QLSingle(n1qlIp, 222);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);
		// cluster.disconnect();

		// executeInsertUserWithItem(cluster);

		// // Move Item Between User Transaction
		// long startTime = System.nanoTime();
		// HashMap<String, String> result =
		// moveItemBetweenUserEmbeddedTransaction(cluster,
		// n1qlIp, "10", "1", "sword");
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// // Move Item Between User No Transaction
		// long startTime = System.nanoTime();
		// HashMap<String, String> result = moveItemBetweenUserEmbedded(cluster,
		// n1qlIp, "10", "1", "sword");
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);
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
			user = selectUserById(randomNumber(1, numberOfUsers),
					"embedded_User");

			JsonObject blog = JsonObject.empty().put("id", b)
					.put("blogpost", randomText(5000))
					.put("user_id", user.get("user_id"))
					.put("vorname", user.get("vorname"))
					.put("nachname", user.get("nachname"))
					.put("email", user.get("email"));

			ArrayList<JsonObject> commentList = new ArrayList<JsonObject>();

			for (c = 1; c <= 3; c++) {

				user = selectUserById(randomNumber(1, numberOfUsers),
						"embedded_User");

				JsonObject comment = JsonObject.empty().put("id", c)
						.put("comment", randomText(2000))
						.put("user_id", user.get("user_id"))
						.put("vorname", user.get("vorname"))
						.put("nachname", user.get("nachname"))
						.put("email", user.get("email"));

				ArrayList<JsonObject> likeList = new ArrayList<JsonObject>();

				for (l = 1; l <= 3; l++) {

					user = selectUserById(randomNumber(1, numberOfUsers),
							"embedded_User");

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

		int b;
		int c;
		int l;

		// Create User and Insert
		for (b = 1; b <= numberOfBlogs; b++) {

			JsonObject blog = JsonObject
					.empty()
					.put("id", b)
					.put("blogpost", randomText(5000))
					.put("user_id", randomNumber(1, numberOfUsers))
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
						.put("user_id", randomNumber(1, numberOfUsers))
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
							.put("user_id", randomNumber(1, numberOfUsers))
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

	public static void executeInsertUserWithItem(Cluster cluster) {

		// Helper Variable
		String userbucket = "embedded_User";
		int count;

		// Get Buckets
		Bucket userBucket = cluster.openBucket(userbucket, "");

		// Insert User
		List<String> items = new ArrayList<String>();
		items.add("laser");
		items.add("sword");
		for (count = 0; count < numberOfUsers; count++) {
			JsonObject user = JsonObject
					.empty()
					.put("id", count + 1)
					.put("item", items)
					.put("vorname", randomText(100))
					.put("nachname", randomText(150))
					.put("email",
							randomText(120) + "@" + randomText(20) + "."
									+ randomText(10));
			JsonDocument doc = JsonDocument.create(String.valueOf(count + 1),
					user);
			userBucket.upsert(doc);
		}

	}

	public static void executeInsertReferenced(Cluster cluster) {

		// Helper Variable
		int u;
		int b;
		int c;
		int l;

		// Get Buckets
		Bucket userBucket = cluster.openBucket("referenced_User", "");
		Bucket blogBucket = cluster.openBucket("referenced_Blog", "");
		Bucket commentBucket = cluster.openBucket("referenced_Comment", "");
		Bucket likeBucket = cluster.openBucket("referenced_Likes", "");

		// Insert User
		for (u = 0; u < numberOfUsers; u++) {
			JsonObject user = JsonObject
					.empty()
					.put("user_id", String.valueOf((u + 1)))
					.put("vorname", randomText(100))
					.put("nachname", randomText(150))
					.put("email",
							randomText(120) + "@" + randomText(20) + "."
									+ randomText(10));
			JsonDocument doc = JsonDocument.create(String.valueOf(u + 1), user);
			userBucket.upsert(doc);
		}

		// Insert Likes
		HashMap<String, ArrayList<JsonObject>> likesIdMap = new HashMap<String, ArrayList<JsonObject>>();

		for (l = 0; l < numberOfLikes; l++) {
			String like_id = String.valueOf(l + 1);
			String comment_id = String
					.valueOf(randomNumber(1, numberOfComments));
			JsonObject like = JsonObject
					.empty()
					.put("like_id", like_id)
					.put("comment_id", comment_id)
					.put("user_id",
							String.valueOf(randomNumber(1, numberOfUsers)));

			JsonDocument doc = JsonDocument.create(String.valueOf(l + 1), like);
			likeBucket.upsert(doc);

			// Comment Id List
			JsonObject likeId = JsonObject.empty().put("id", like_id);

			if (likesIdMap.get(comment_id) != null) {
				ArrayList<JsonObject> likesIdList = likesIdMap.get(comment_id);
				likesIdList.add(likeId);
				likesIdMap.put(comment_id, likesIdList);

			} else {

				ArrayList<JsonObject> likesIdList = new ArrayList<JsonObject>();
				likesIdList.add(likeId);
				likesIdMap.put(comment_id, likesIdList);
			}
		}

		// Insert Comment
		HashMap<String, ArrayList<JsonObject>> commentIdMap = new HashMap<String, ArrayList<JsonObject>>();

		for (c = 0; c < numberOfComments; c++) {
			String comment_id = String.valueOf(c + 1);
			String blog_id = String.valueOf(randomNumber(1, numberOfBlogs));
			JsonObject comment = JsonObject
					.empty()
					.put("comment_id", comment_id)
					.put("comment", randomText(5000))
					.put("user_id",
							String.valueOf(randomNumber(1, numberOfUsers)))
					.put("blog_id", blog_id);

			// Push Like Id List on Comment
			if (likesIdMap.get(comment_id) != null) {
				comment.put("like_ids", likesIdMap.get(comment_id));
			}

			JsonDocument doc = JsonDocument.create(String.valueOf(c + 1),
					comment);
			commentBucket.upsert(doc);

			// Blog Id List
			JsonObject commentId = JsonObject.empty().put("id", comment_id);

			if (commentIdMap.get(blog_id) != null) {
				ArrayList<JsonObject> commentIdList = commentIdMap.get(blog_id);
				commentIdList.add(commentId);
				commentIdMap.put(blog_id, commentIdList);

			} else {

				ArrayList<JsonObject> commentIdList = new ArrayList<JsonObject>();
				commentIdList.add(commentId);
				commentIdMap.put(blog_id, commentIdList);
			}
		}

		// Insert Blog
		for (b = 0; b < numberOfBlogs; b++) {
			String blog_id = String.valueOf((b + 1));
			JsonObject blog = JsonObject
					.empty()
					.put("blog_id", blog_id)
					.put("blogpost", randomText(5000))
					.put("user_id",
							String.valueOf(randomNumber(1, numberOfUsers)));

			// Push Comment Id List on Blog
			if (commentIdMap.get(blog_id) != null) {
				blog.put("comment_ids", commentIdMap.get(blog_id));
			}

			// Perform Insert
			JsonDocument doc = JsonDocument.create(String.valueOf(b + 1), blog);
			blogBucket.upsert(doc);
		}

	}

	// Helper Method
	private static Map<String, String> selectUserById(int id, String bucketName) {

		Map<String, String> resultSet = null;
		DefaultHttpClient httpclient = new DefaultHttpClient();

		try {
			// specify the host, protocol, and port
			HttpHost target = new HttpHost("192.168.122.57", 8093, "http");

			// specify the get request
			HttpGet getRequest = new HttpGet(
					"/query?statement=SELECT%20*%20FROM%20" + bucketName
							+ "%20WHERE%20id=" + id);

			HttpResponse httpResponse = httpclient.execute(target, getRequest);
			HttpEntity entity = httpResponse.getEntity();

			if (entity != null) {
				String retSrc = EntityUtils.toString(entity);
				// parsing JSON
				JSONObject result = new JSONObject(retSrc); // Convert String to
															// JSON Object

				JSONArray userList = result.getJSONArray("results");
				JSONObject user = userList.getJSONObject(0).getJSONObject(
						bucketName);

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

	public static ArrayList<HashMap<String, String>> selectBlogWithAssociatesEmbeddedN1QL(
			String ip) {

		ArrayList<HashMap<String, String>> result = new ArrayList<HashMap<String, String>>();
		HashMap<String, String> resultSet = null;
		DefaultHttpClient httpclient = new DefaultHttpClient();

		// Helper
		String embedded_Blog = "embedded_Blog";

		try {
			// specify the host, protocol, and port
			HttpHost target = new HttpHost(ip, 8093, "http");

			int i;
			for (i = 1; i <= 10; i++) {

				HttpGet getRequest = new HttpGet(
						"/query?statement=SELECT%20*%20FROM%20" + embedded_Blog
								+ "%20WHERE%20id=" + i);
				HttpResponse httpResponse = httpclient.execute(target,
						getRequest);
				HttpEntity entity = httpResponse.getEntity();

				if (entity != null) {
					String retSrc = EntityUtils.toString(entity);
					// parsing JSON
					JSONObject response = new JSONObject(retSrc); // Convert
																	// String to
																	// JSON
																	// Object

					JSONArray blogList = response.getJSONArray("results");
					JSONObject blog = blogList.getJSONObject(0).getJSONObject(
							embedded_Blog);

					// Parse Result Set
					resultSet = new HashMap<String, String>();
					resultSet.put("id", String.valueOf(blog.getInt("id")));
					result.add(resultSet);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// When HttpClient instance is no longer needed,
			// shut down the connection manager to ensure
			// immediate deallocation of all system resources
			httpclient.getConnectionManager().shutdown();
		}

		return result;
	}

	public static HashMap<String, String> selectBlogWithAssociatesEmbeddedN1QLSingle(
			String ip, Integer id) {

		HashMap<String, String> resultSet = null;
		DefaultHttpClient httpclient = new DefaultHttpClient();

		// Helper
		String embedded_Blog = "embedded_Blog";

		try {
			// specify the host, protocol, and port
			HttpHost target = new HttpHost(ip, 8093, "http");

			HttpGet getRequest = new HttpGet(
					"/query?statement=SELECT%20*%20FROM%20" + embedded_Blog
							+ "%20WHERE%20id=" + id);
			HttpResponse httpResponse = httpclient.execute(target, getRequest);
			HttpEntity entity = httpResponse.getEntity();

			if (entity != null) {
				String retSrc = EntityUtils.toString(entity);
				// parsing JSON
				JSONObject response = new JSONObject(retSrc); // Convert
																// String to
																// JSON
																// Object

				JSONArray blogList = response.getJSONArray("results");
				JSONObject blog = blogList.getJSONObject(0).getJSONObject(
						embedded_Blog);

				// Parse Result Set
				resultSet = new HashMap<String, String>();
				resultSet.put("id", String.valueOf(blog.getInt("id")));

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

	public static ArrayList<HashMap<String, String>> selectBlogWithAssociatesReferencedN1QL(
			String ip) {

		ArrayList<HashMap<String, String>> result = new ArrayList<HashMap<String, String>>();
		HashMap<String, String> resultSet = null;
		DefaultHttpClient httpclient = new DefaultHttpClient();

		// Helper
		String referenced_Blog = "referenced_Blog";
		String referenced_Comment = "referenced_Comment";
		String referenced_User = "referenced_User";
		String referenced_Like = "referenced_Like";

		try {
			// specify the host, protocol, and port
			HttpHost target = new HttpHost(ip, 8093, "http");

			int i;
			for (i = 1; i <= numberOfBlogs; i++) {

				HttpGet getRequest = new HttpGet(
						"/query?statement=SELECT%20*%20FROM%20"
								+ referenced_Blog
								+ "%20rb%20LEFT%20JOIN%20"
								+ referenced_Comment
								+ "%20rc%20ON%20KEYS%20ARRAY%20c.id%20FOR%20c%20IN%20rb.comment_ids%20END%20JOIN%20"
								+ referenced_User
								+ "%20rub%20ON%20KEYS%20rb.user_id%20LEFT%20JOIN%20"
								+ referenced_User
								+ "%20ruc%20ON%20KEYS%20rc.user_id%20LEFT%20JOIN%20"
								+ referenced_Like
								+ "%20rl%20ON%20KEYS%20%20ARRAY%20l.id%20FOR%20l%20IN%20rc.like_ids%20END%20LEFT%20JOIN%20"
								+ referenced_User
								+ "%20rul%20ON%20KEYS%20rl.user_id%20WHERE%20rb.blog_id%20='"
								+ i + "'");

				HttpResponse httpResponse = httpclient.execute(target,
						getRequest);
				HttpEntity entity = httpResponse.getEntity();

				if (entity != null) {
					String retSrc = EntityUtils.toString(entity);
					// parsing JSON
					JSONObject response = new JSONObject(retSrc); // Convert
																	// String to
																	// JSON
																	// Object

					JSONArray blogList = response.getJSONArray("results");
					JSONObject blog = blogList.getJSONObject(0).getJSONObject(
							"rb");

					// Parse Result Set
					resultSet = new HashMap<String, String>();
					resultSet.put("id", String.valueOf(blog.getInt("blog_id")));
					result.add(resultSet);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// When HttpClient instance is no longer needed,
			// shut down the connection manager to ensure
			// immediate deallocation of all system resources
			httpclient.getConnectionManager().shutdown();
		}

		return result;
	}

	public static HashMap<String, String> selectBlogWithAssociatesReferencedN1QLSingle(
			String ip, Integer id) {

		HashMap<String, String> resultSet = null;
		DefaultHttpClient httpclient = new DefaultHttpClient();

		try {
			// specify the host, protocol, and port
			HttpHost target = new HttpHost(ip, 8093, "http");
			String referenced_Blog = "referenced_Blog";
			String referenced_Comment = "referenced_Comment";
			String referenced_User = "referenced_User";
			String referenced_Like = "referenced_Likes";

			HttpGet getRequest = new HttpGet(
					"/query?statement=SELECT%20*%20FROM%20"
							+ referenced_Blog
							+ "%20rb%20LEFT%20JOIN%20"
							+ referenced_Comment
							+ "%20rc%20ON%20KEYS%20ARRAY%20c.id%20FOR%20c%20IN%20rb.comment_ids%20END%20JOIN%20"
							+ referenced_User
							+ "%20rub%20ON%20KEYS%20rb.user_id%20LEFT%20JOIN%20"
							+ referenced_User
							+ "%20ruc%20ON%20KEYS%20rc.user_id%20LEFT%20JOIN%20"
							+ referenced_Like
							+ "%20rl%20ON%20KEYS%20%20ARRAY%20l.id%20FOR%20l%20IN%20rc.like_ids%20END%20LEFT%20JOIN%20"
							+ referenced_User
							+ "%20rul%20ON%20KEYS%20rl.user_id%20WHERE%20rb.blog_id%20='"
							+ id + "'");
			HttpResponse httpResponse = httpclient.execute(target, getRequest);
			HttpEntity entity = httpResponse.getEntity();

			if (entity != null) {
				String retSrc = EntityUtils.toString(entity);
				// parsing JSON
				JSONObject response = new JSONObject(retSrc); // Convert
																// String to
																// JSON
																// Object

				System.out.println(response);

				JSONArray blogList = response.getJSONArray("results");
				JSONObject blog = blogList.getJSONObject(0).getJSONObject("rb");

				// Parse Result Set
				resultSet = new HashMap<String, String>();
				resultSet.put("id", String.valueOf(blog.getInt("blog_id")));
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

	public static HashMap<String, String> tagTopBloggerReferencedN1QL(String ip) {

		HashMap<String, String> resultSet = new HashMap<String, String>();
		DefaultHttpClient httpclient = new DefaultHttpClient();

		try {
			// specify the host, protocol, and port
			HttpHost target = new HttpHost(ip, 8093, "http");

			// HELPER
			String referenced_Blog = "referenced_Blog";
			String referenced_User = "referenced_User";

			HttpGet getRequest = new HttpGet(
					"/query?statement=SELECT%20user_id%20FROM%20"
							+ referenced_Blog
							+ "%20GROUP%20BY%20user_id%20ORDER%20BY%20count(user_id)%20DESC,%20user_id%20LIMIT%201;");
			HttpResponse httpResponse = httpclient.execute(target, getRequest);
			HttpEntity entity = httpResponse.getEntity();

			if (entity != null) {
				String retSrc = EntityUtils.toString(entity);
				// parsing JSON
				JSONObject response = new JSONObject(retSrc); // Convert
																// String to
																// JSON
																// Object

				resultSet.put("result", response.toString());

				JSONArray blogList = response.getJSONArray("results");
				String topBlogger = blogList.getJSONObject(0).get("user_id")
						.toString();

				HttpPost postRequest = new HttpPost(
						"/query?statement=UPDATE%20"
								+ referenced_User
								+ "%20SET%20rank%20=%20'TopBlogger'%20WHERE%20user_id%20=%20'"
								+ topBlogger + "';");

				httpResponse = httpclient.execute(target, postRequest);
				entity = httpResponse.getEntity();

				if (entity != null) {
					retSrc = EntityUtils.toString(entity);
					response = new JSONObject(retSrc);
				}

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

	public static HashMap<String, String> tagTopBloggerEmbeddedN1QL(String ip) {

		HashMap<String, String> resultSet = null;
		DefaultHttpClient httpclient = new DefaultHttpClient();

		// HELPER
		String embedded_Blog = "embedded_Blog";

		try {
			// specify the host, protocol, and port
			HttpHost target = new HttpHost(ip, 8093, "http");

			HttpGet getRequest = new HttpGet(
					"/query?statement=SELECT%20user_id%20FROM%20"
							+ embedded_Blog
							+ "%20GROUP%20BY%20user_id%20ORDER%20BY%20count(user_id)%20DESC,%20id%20LIMIT%201;");
			HttpResponse httpResponse = httpclient.execute(target, getRequest);
			HttpEntity entity = httpResponse.getEntity();

			if (entity != null) {
				String retSrc = EntityUtils.toString(entity);
				// parsing JSON
				JSONObject response = new JSONObject(retSrc); // Convert
																// String to
																// JSON
																// Object

				JSONArray blogList = response.getJSONArray("results");
				String topBlogger = blogList.getJSONObject(0).get("user_id")
						.toString();

				System.out.println(topBlogger);

				// Update Blog
				HttpPost postRequest = new HttpPost(
						"/query?statement=UPDATE%20"
								+ embedded_Blog
								+ "%20SET%20rank%20=%20'TopBlogger'%20WHERE%20user_id%20=%20"
								+ topBlogger + ";");

				httpResponse = httpclient.execute(target, postRequest);
				entity = httpResponse.getEntity();

				if (entity != null) {
					retSrc = EntityUtils.toString(entity);
					response = new JSONObject(retSrc);

					System.out.println(response);
				}

				// Update Comment
				postRequest = new HttpPost(
						"/query?statement=UPDATE%20"
								+ embedded_Blog
								+ "%20SET%20c.rank%20=%20'TopBlogger'%20FOR%20c%20IN%20Comment%20WHEN%20c.user_id%20=%20"
								+ topBlogger + "%20END;");

				httpResponse = httpclient.execute(target, postRequest);
				entity = httpResponse.getEntity();

				if (entity != null) {
					retSrc = EntityUtils.toString(entity);
					response = new JSONObject(retSrc);

					System.out.println(response);
				}

				// Update Like
				// TODO: Implement
				// Dummy
				postRequest = new HttpPost(
						"/query?statement=UPDATE%20"
								+ embedded_Blog
								+ "%20SET%20c.rank%20=%20'TopBlogger'%20FOR%20c%20IN%20Comment%20WHEN%20c.user_id%20=%20"
								+ topBlogger + "%20END;");

				httpResponse = httpclient.execute(target, postRequest);
				entity = httpResponse.getEntity();

				if (entity != null) {
					retSrc = EntityUtils.toString(entity);
					response = new JSONObject(retSrc);

					System.out.println(response);
				}

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

	public static HashMap<String, String> moveItemBetweenUserEmbeddedTransaction(
			Cluster cluster, String ip, String fromUserId, String toUserId,
			String transactionItem) {

		HashMap<String, String> resultSet = null;
		DefaultHttpClient httpclient = new DefaultHttpClient();

		Bucket userBucket = cluster.openBucket("embedded_User", "");
		Bucket transactionBucket = cluster.openBucket("embedded_Transaction",
				"");

		// Create Transaction Object and set state to INIT
		JsonObject transaction = JsonObject.empty().put("id", "1")
				.put("from", fromUserId).put("to", toUserId)
				.put("item", transactionItem).put("state", "init");
		JsonDocument doc = JsonDocument.create(String.valueOf(1), transaction);
		transactionBucket.upsert(doc);

		// Retrieve Transaction Object
		transaction = transactionBucket.get("1").content();
		String from = transaction.getString("from");
		String to = transaction.getString("to");
		String item = transaction.getString("item");

		// Set State to PENDING
		transaction.put("state", "pending");
		doc = JsonDocument.create(String.valueOf(1), transaction);
		transactionBucket.upsert(doc);

		// Update First Document and Set Transaction to 1
		// Update the Item List (Loop through old List and create the new one
		// without transaction item
		JsonObject fromUser = userBucket.get(from).content();
		List<String> myList = new ArrayList<String>();
		List<Object> itemObjects = fromUser.getArray("item").toList();
		for (Object a : itemObjects) {
			if (!a.toString().equals(item)) {
				myList.add(a.toString());
			}
		}
		fromUser.put("item", myList);
		fromUser.put("transaction", "1");
		doc = JsonDocument.create(String.valueOf(from), fromUser);
		userBucket.upsert(doc);

		// Update Second Document and Set Transaction to 1
		JsonObject toUser = userBucket.get(to).content();
		toUser.getArray("item").add(item);
		toUser.put("transaction", "1");
		doc = JsonDocument.create(String.valueOf(to), toUser);
		userBucket.upsert(doc);

		// Retrieve Transaction Object and set State to COMMITTED
		transaction = transactionBucket.get("1").content();
		transaction.put("state", "committed");
		doc = JsonDocument.create(String.valueOf(1), transaction);
		transactionBucket.upsert(doc);

		// Update First Document and Set Transaction to 0
		fromUser = userBucket.get(from).content();
		fromUser.put("transaction", "0");
		doc = JsonDocument.create(String.valueOf(from), fromUser);
		userBucket.upsert(doc);

		// Update Second Document and Set Transaction to 0
		toUser = userBucket.get(to).content();
		toUser.put("transaction", "0");
		doc = JsonDocument.create(String.valueOf(to), toUser);
		userBucket.upsert(doc);

		// Retrieve Transaction Object and set State to DONE
		transaction = transactionBucket.get("1").content();
		transaction.put("state", "done");
		doc = JsonDocument.create(String.valueOf(1), transaction);
		transactionBucket.upsert(doc);

		return resultSet;

	}

	public static HashMap<String, String> moveItemBetweenUserEmbedded(
			Cluster cluster, String ip, String fromUserId, String toUserId,
			String transactionItem) {

		HashMap<String, String> resultSet = null;
		DefaultHttpClient httpclient = new DefaultHttpClient();

		Bucket userBucket = cluster.openBucket("embedded_User", "");
		String from = "2";
		String to = "1";
		String item = "laser";

		// Update First Document
		// Update the Item List (Loop through old List and create the new one
		// without transaction item
		JsonObject fromUser = userBucket.get(from).content();
		List<String> myList = new ArrayList<String>();
		List<Object> itemObjects = fromUser.getArray("item").toList();
		for (Object a : itemObjects) {
			if (!a.toString().equals(item)) {
				myList.add(a.toString());
			}
		}
		fromUser.put("item", myList);
		JsonDocument doc = JsonDocument.create(String.valueOf(from), fromUser);
		userBucket.upsert(doc);

		// Update Second Document
		JsonObject toUser = userBucket.get(to).content();
		toUser.getArray("item").add(item);
		doc = JsonDocument.create(String.valueOf(to), toUser);
		userBucket.upsert(doc);

		return resultSet;

	}

}