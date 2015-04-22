package net.ripe.hadoop.pcap.io.reader;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;

import net.ripe.hadoop.pcap.PcapReader;
import net.ripe.hadoop.pcap.packet.KeyWritable;
import net.ripe.hadoop.pcap.packet.Packet;
import net.ripe.hadoop.pcap.packet.ValueWritable;

import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class PcapRecordReader extends RecordReader<KeyWritable, ValueWritable> {

	PcapReader pcapReader;
	Iterator<Packet> pcapReaderIterator;
	Seekable baseStream;
	DataInputStream stream;

	long packetCount = 0;
	long start, end;

	private KeyWritable key = new KeyWritable();

	private ValueWritable value = new ValueWritable();

	public PcapRecordReader(PcapReader pcapReader, long start, long end,
			Seekable baseStream, DataInputStream stream) throws IOException {
		this.pcapReader = pcapReader;
		this.baseStream = baseStream;
		this.stream = stream;
		this.start = start;
		this.end = end;

		if (pcapReader != null) {
			pcapReaderIterator = pcapReader.iterator();
		}
	}

	public String toString() {
		return pcapReader.iterator().toString();
	}

	Packet packet = null;

	boolean values = true;

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		while (values) {
			if (pcapReaderIterator.hasNext()) {

				packet = pcapReaderIterator.next();

				if (packet.get(Packet.SRC) != null
						&& packet.get(Packet.TIMESTAMP) != null
						&& packet.get(Packet.DST) != null
						&& packet.get(Packet.SRC_PORT) != null
						&& packet.get(Packet.DST_PORT) != null
						&& packet.get(Packet.TOTAL_SIZE) != null) {

					key.setClientIp(new Text(packet.get(Packet.SRC).toString()));
					key.setTimestamp(new Text(packet.get(Packet.TIMESTAMP)
							.toString()));

					value.setClientIp(new Text(packet.get(Packet.SRC)
							.toString()));
					value.setServerIp(new Text(packet.get(Packet.DST)
							.toString()));
					value.setTimeStamp(new Text(packet.get(Packet.TIMESTAMP)
							.toString()));
					value.setSrcPort(new Text(packet.get(Packet.SRC_PORT)
							.toString()));
					value.setDstPort(new Text(packet.get(Packet.DST_PORT)
							.toString()));
					value.setDownloadTraffic(new Text(packet.get(
							Packet.TOTAL_SIZE).toString()));
				} else {
					packet.clear();
					continue;
				}

				return true;
			}

			values = false;
		}

		return false;
	}

	@Override
	public KeyWritable getCurrentKey() throws IOException, InterruptedException {

		return key;
	}

	@Override
	public ValueWritable getCurrentValue() throws IOException,
			InterruptedException {

		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		if (start == end)
			return 0;
		return Math.min(1.0f, (getPos() - start) / (float) (end - start));
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {

	}

	public long getPos() throws IOException {
		return baseStream.getPos();
	}

	@Override
	public void close() throws IOException {
		stream.close();
	}

}