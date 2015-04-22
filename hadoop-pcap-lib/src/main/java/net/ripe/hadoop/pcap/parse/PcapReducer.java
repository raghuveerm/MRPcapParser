package net.ripe.hadoop.pcap.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.ripe.hadoop.pcap.packet.KeyWritable;
import net.ripe.hadoop.pcap.packet.ValueWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.calsoftlabs.mongo.upload.MongoUpload;

public class PcapReducer extends
		Reducer<KeyWritable, ValueWritable, KeyWritable, Text> {

	private static final Long KILOBYTE = 1024L;

	private static final MongoUpload mongoUplaod = new MongoUpload();

	int i = 0;
	int srcPort_repetition = 0;
	int dstPort_repetition = 0;

	String ip = null;

	Long sum = null;
	Long kbs = null;

	List<String> sIp = null;
	List<String> dIp = null;
	List<String> srPort = null;
	List<String> dsPort = null;
	List<String> tts = null;

	public void reduce(KeyWritable key, Iterable<ValueWritable> values,
			Context context) throws IOException, InterruptedException {

		List<String> clientIp = new ArrayList<String>();

		List<String> serverIp = new ArrayList<String>();

		List<String> srcPort = new ArrayList<String>();

		List<String> destPort = new ArrayList<String>();

		List<String> timestamp = new ArrayList<String>();

		sum = new Long("0");

		for (ValueWritable val : values) {

			clientIp.add(val.getClientIp().toString());
			serverIp.add(val.getServerIp().toString());
			srcPort.add(val.getSrcPort().toString());
			destPort.add(val.getDstPort().toString());
			timestamp.add(val.getTimeStamp().toString());

			if (val.getDownloadTraffic().toString().trim() != null) {

				sum = sum
						+ Long.parseLong(val.getDownloadTraffic().toString()
								.trim());
			}

		}

		Set<String> sourceIp = new HashSet<String>(clientIp);
		Set<String> destIp = new HashSet<String>(serverIp);
		Set<String> sPort = new HashSet<String>(srcPort);
		Set<String> dPort = new HashSet<String>(destPort);
		Set<String> time = new HashSet<String>(timestamp);
		
		
		ip = sourceIp.toString().replaceAll("\\[", " ")
				.replaceAll("\\]", "").trim();

		kbs = (sum / KILOBYTE);

		srcPort_repetition = srcPort.size();
		dstPort_repetition = destPort.size();

		sIp = new ArrayList<String>();
		sIp.add(sourceIp.toString().replaceAll("\\[", " ")
				.replaceAll("\\]", "").trim());
		dIp = new ArrayList<String>();
		dIp.add(destIp.toString().replaceAll("\\[", " ").replaceAll("\\]", "")
				.trim());
		srPort = new ArrayList<String>();
		srPort.add(sPort.toString().replaceAll("\\[", " ")
				.replaceAll("\\]", "").trim());
		dsPort = new ArrayList<String>();
		dsPort.add(dPort.toString().replaceAll("\\[", " ")
				.replaceAll("\\]", "").trim());
		tts = new ArrayList<String>();
		tts.add(time.toString().replaceAll("\\[", " ").replaceAll("\\]", "")
				.trim());

		statusCheck(sIp, dIp, srPort, dsPort, tts, kbs, srcPort_repetition,
				dstPort_repetition, ip);

	}

	public void statusCheck(List<String> sourceIp, List<String> destIp,
			List<String> srcPort, List<String> dstPort,
			List<String> timestamp, Long kbs, int srcPort_repetition,
			int dstPort_repetition, String sIp) {

		mongoUplaod.insertRecordsToMongo(sourceIp, destIp, srcPort, dstPort,
				timestamp, kbs, srcPort_repetition, dstPort_repetition, sIp);

	}

}
