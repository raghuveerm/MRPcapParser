package net.ripe.hadoop.pcap.parse;


import org.apache.hadoop.util.ProgramDriver;



public class PcapMain {

	public static void main(String[] args) throws Exception {

		int exitCode = -1;

		ProgramDriver pgd = new ProgramDriver();

		try {
			pgd.addClass("pcap", PcapJobRunner.class,
					"A map/reduce program that samples the schemas for every 5 msec for a particular client download");

			
			// pgd.addClass("pcap", PcapJobRunner.class,
			// "A map/reduce program that samples the schemas for every 5 msec for a particular client upload");
			exitCode = pgd.run(args);

		} catch (Throwable e) {

			e.printStackTrace();
		}
		System.exit(exitCode);
	}

}
