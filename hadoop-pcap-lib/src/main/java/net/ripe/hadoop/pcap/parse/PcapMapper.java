package net.ripe.hadoop.pcap.parse;


import java.io.IOException;

import net.ripe.hadoop.pcap.packet.KeyWritable;
import net.ripe.hadoop.pcap.packet.ValueWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PcapMapper extends Mapper<KeyWritable, ValueWritable, KeyWritable, ValueWritable> {
	
	
	
	private static final KeyWritable KEY = new KeyWritable();
	private static final ValueWritable VALUE = new ValueWritable();


	public void map(KeyWritable key, ValueWritable value, Context context) throws IOException,
			InterruptedException {
		
	
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
		

		context.write(KEY,VALUE);
	}
}
