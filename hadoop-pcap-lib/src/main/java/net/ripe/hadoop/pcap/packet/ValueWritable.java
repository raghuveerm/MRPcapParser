package net.ripe.hadoop.pcap.packet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ValueWritable implements Writable {

	private Text clientIp;
	private Text serverIp;
	private Text downloadTraffic;
	private Text srcPort;
	private Text dstPort;
	private Text timeStamp;

	public ValueWritable() {

		this.clientIp = new Text();
		this.serverIp = new Text();
		this.downloadTraffic = new Text();
		this.srcPort = new Text();
		this.dstPort = new Text();
		this.timeStamp = new Text();

	}

	public ValueWritable(Text clientIp, Text serverIp, Text downloadTraffic, Text srcPort,
			Text dstPort, Text timeStamp) {

		this.clientIp = clientIp;
		this.serverIp = serverIp;
		this.downloadTraffic = downloadTraffic;
		this.srcPort = srcPort;
		this.dstPort = dstPort;
		this.timeStamp = timeStamp;

	}

	public void readFields(DataInput in) throws IOException {

		clientIp.readFields(in);
		serverIp.readFields(in);
		downloadTraffic.readFields(in);
		srcPort.readFields(in);
		dstPort.readFields(in);
		timeStamp.readFields(in);

	}

	public void write(DataOutput out) throws IOException {

		clientIp.write(out);
		serverIp.write(out);
		downloadTraffic.write(out);
		srcPort.write(out);
		dstPort.write(out);
		timeStamp.write(out);

	}

	// getters and setters

	public Text getClientIp() {
		return clientIp;
	}

	public void setClientIp(Text clientIp) {
		this.clientIp = clientIp;
	}

	public Text getServerIp() {
		return serverIp;
	}

	public void setServerIp(Text serverIp) {
		this.serverIp = serverIp;
	}

	public Text getDownloadTraffic() {
		return downloadTraffic;
	}

	public void setDownloadTraffic(Text downloadTraffic) {
		this.downloadTraffic = downloadTraffic;
	}

	public Text getSrcPort() {
		return srcPort;
	}

	public void setSrcPort(Text srcPort) {
		this.srcPort = srcPort;
	}

	public Text getDstPort() {
		return dstPort;
	}

	public void setDstPort(Text dstPort) {
		this.dstPort = dstPort;
	}

	public Text getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Text timeStamp) {
		this.timeStamp = timeStamp;
	}

	@Override
	public String toString() {

		String client_IP = clientIp.toString();
		String server_Ip = serverIp.toString();
		String traffic = downloadTraffic.toString();
		String src_Port = srcPort.toString();
		String dst_Port = dstPort.toString();
		String time = timeStamp.toString();
		String data = client_IP + "," + server_Ip + "," + time + "," + traffic + "," + src_Port
				+ "," + dst_Port;

		return data;
	}

}