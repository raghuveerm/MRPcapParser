 package net.ripe.hadoop.pcap.mongo.upload;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import net.ripe.hadoop.pcap.location.GeoLocation;
import net.ripe.hadoop.pcap.schema.Schema;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoUpload {

	private DBCollection collection = null;

	private MongoClient client = null;

	private static final MongoClientURI MONGO_URI = new MongoClientURI(
			"mongodb://192.168.7.102:27017");

	String city;

	String code;

	String cityCode;

	private static final GeoLocation obj = new GeoLocation();

	public void insertRecordsToMongo(List<String> sourceIp,
			List<String> destIp, List<String> srcPort, List<String> dstPort,
			List<String> timestamp, Long kbs, int srcPort_repetition,
			int dstPort_repetition, String clientIp) {

		String srcIp = clientIp.replaceAll("\\[", " ").replaceAll("\\]", "");

		String[] ips = srcIp.split(",");

		try {

			cityCode = obj.getLocation(ips[0].toString());

			String[] value = cityCode.toString().split(",");

			city = value[0];
			code = value[1].replaceAll("\\(", "").replaceAll("\\)", "");

		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {

			if (getCollection() == null) {

				this.collection = getcollectionFromFactory(Schema.DOWNLOAD);
			}

			String key = null;
			String ip1 = null;
			String ip2 = null;
			String tts = null;
			String sPort = null;
			String dPort = null;

			for (int i = 0; i < timestamp.size(); i++) {

				key = sourceIp.get(i) + "_" + destIp.get(i) + "_"
						+ timestamp.get(i);
				ip1 = sourceIp.get(i);
				ip2 = destIp.get(i);
				tts = timestamp.get(i);
				sPort = srcPort.get(i);
				dPort = dstPort.get(i);

			}

			DBObject document = new BasicDBObject("_id", key)
					.append("client_ip", ip1)
					.append("server_ip", ip2)
					.append("timestamp", tts)
					.append("bytes_transfered", Integer.valueOf(kbs.toString()))
					.append("src_port", sPort).append("dst_port", dPort)
					.append("country_name", city).append("country_code", code);

			collection.insert(document);

		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	private DBCollection getcollectionFromFactory(Schema collectionName) {

		switch (collectionName) {

		case UPLOAD:
			collection = createcollection("upload", "c2s");
			break;

		case DOWNLOAD:
			collection = createcollection("download", "s2c");
			break;

		default:
			collection = createcollection("calsoftlabs", "persons");
			System.out.println("Your data is inserted into the TEST DB ... ");
			break;
		}

		return collection;

	}

	public DBCollection createcollection(String dbName, String collection) {

		try {

			this.client = new MongoClient(MONGO_URI);
			this.collection = getClient().getDB(dbName).getCollection(
					collection);

		} catch (UnknownHostException IP) {
			System.out.println("Host IP is Incorrect" + IP);
		} catch (Exception e) {
			System.out
					.println("Unable to connect kindly check server status and collection limit");

		}

		return getCollection();
	}

	public DBCollection getCollection() {
		return collection;
	}

	public void setCollection(DBCollection collection) {
		this.collection = collection;
	}

	public MongoClient getClient() {
		return client;
	}

	public void setClient(MongoClient client) {
		this.client = client;
	}
}