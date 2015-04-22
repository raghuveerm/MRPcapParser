package com.calsoftlabs.pcap.parse;

import java.io.IOException;

import net.ripe.hadoop.pcap.packet.KeyWritable;
import net.ripe.hadoop.pcap.packet.ValueWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PcapMapper extends Mapper<KeyWritable, ValueWritable, KeyWritable, ValueWritable> {
	
	//private static final KeyWritable KEYWRITABLE = new KeyWritable();
	
	private static final KeyWritable KEY = new KeyWritable();
	private static final ValueWritable VALUE = new ValueWritable();
//	private static final PacketBean BEAN = new PacketBean(); 

	public void map(KeyWritable key, ValueWritable value, Context context) throws IOException,
			InterruptedException {
		
	

//		 Packet packet = (Packet) value.get();
		
		
		 
//		 int sPort_repetition = 0;
//		 int dPort_repetition = 0;
		
		/* List<String> srcIp = new ArrayList<String>();
		 List<String> sPort = new ArrayList<String>();
		 List<String> destIp = new ArrayList<String>();
		 List<String> dPort = new ArrayList<String>();
		 List<String> time = new ArrayList<String>();
		 List<Integer> bytes = new ArrayList<Integer>();
		
		*/
//		 if (value != null) {
//		 srcIp.add(value.getSourceIp().toString());
//		 destIp.add(value.getServerIp().toString());
//		 sPort.add(value.getSrcPort().toString());
//		 dPort.add(value.getDstPort().toString());
//		 time.add(value.getTimeStamp().toString());
//		 bytes.add(Integer.valueOf(value.getDownloadTraffic().toString()));
//		
//		 }
		
//		 int sum = 0;
//		
//		 for(int val : bytes) {
//		
//		 sum += val;
//		
//		 }
//		
		
		Text client_Ip = key.getClientIp();
		Text timeStamp = key.getTimestamp();
		
		KEY.setClientIp(client_Ip);
		KEY.setTimestamp(timeStamp);
		
		
		Text clientIp = value.getClientIp();
		Text serverIp = value.getServerIp();
		Text srcPort = value.getSrcPort();
		Text destPort = value.getDstPort();
		Text timestamp = value.getTimeStamp();
		Text traffic = value.getDownloadTraffic();
		
		VALUE.setClientIp(clientIp);
		VALUE.setServerIp(serverIp);
		VALUE.setSrcPort(srcPort);
		VALUE.setDstPort(destPort);
		VALUE.setTimeStamp(timestamp);
		VALUE.setDownloadTraffic(traffic);
		
//		BEAN.setSrcPort(srcPort.toString());
//		BEAN.setDestPort(destPort.toString());
//		BEAN.setBytes_transfered(traffic.toString());

		context.write(KEY,VALUE);
	}
}
