package net.ripe.hadoop.pcap.packet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class KeyWritable implements WritableComparable<KeyWritable> {

	private Text timestamp;
	private Text clientIp;
	

	
	
	public KeyWritable() {

		timestamp = new Text();
		clientIp = new Text();
		
	}

	public KeyWritable(Text timestamp,Text clientIp) {

		this.timestamp = timestamp;
		this.clientIp = clientIp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		timestamp.readFields(in);
		clientIp.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {

		timestamp.write(out);
		clientIp.write(out);

	}

	@Override
	public int compareTo(KeyWritable o) {

		KeyWritable other = (KeyWritable) o;
		int cmp = clientIp.compareTo(other.clientIp);
		

		if (cmp != 0) {
			return cmp;
		}
		return timestamp.compareTo(other.timestamp);
	}

	@Override
	public boolean equals(Object o) {

		if (o == null) {
			return false;
		}

		if (o instanceof KeyWritable) {
			KeyWritable other = (KeyWritable) o;
			return clientIp.equals(other.clientIp) && timestamp.equals(other.timestamp);
		}
		return false;

	}

	public Text getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Text timestamp) {
		this.timestamp = timestamp;
	}
	
	public Text getClientIp() {
		return clientIp;
	}

	public void setClientIp(Text clientIp) {
		this.clientIp = clientIp;
	}

	


	@Override
	public String toString() {

		String timestamp = getTimestamp().toString();
		String clientIp = getClientIp().toString();
	
		return clientIp+"_"+timestamp;

	}

}
