package net.ripe.hadoop.pcap.parse;

import java.io.IOException;

import net.ripe.hadoop.pcap.io.CombineBinaryInputFormat;
import net.ripe.hadoop.pcap.io.PcapInputFormat;
import net.ripe.hadoop.pcap.packet.KeyWritable;
import net.ripe.hadoop.pcap.packet.ValueWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author divya
 * 
 *         This is the main driver for ClientTrafficEstimation map/reduce
 *         program. Invokes run() method to submit the map/reduce job.
 * @throws IOException
 *             When there is communication problems with the job tracker.
 */
public class PcapJobRunner extends Configured implements Tool {

	public static void main(String[] args) throws IOException, Exception {

		int res = ToolRunner
				.run(new Configuration(), new PcapJobRunner(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJobName("pcap");

		job.setJarByClass(PcapJobRunner.class);

		job.setOutputKeyClass(KeyWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(KeyWritable.class);
		job.setMapOutputValueClass(ValueWritable.class);

		job.setMapperClass(PcapMapper.class);
	    job.setReducerClass(PcapReducer.class);

		job.setInputFormatClass(CombineBinaryInputFormat.class);
		// CombineBinaryInputFormat.setMaxInputSplitSize(job, (long)67108864);
		// //64MB

		job.setNumReduceTasks(2);
		
		job.setProfileEnabled(true);
		job.setProfileParams("-agentlib:hprof=cpu=samples," + "heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s");
		job.setProfileTaskRange(true, "0-2");
		job.setProfileTaskRange(false, "0-2");
		

		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

}
