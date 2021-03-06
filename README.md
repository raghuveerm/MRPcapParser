Hadoop PCAP library
===================

Parsing large number of small files in binary format. Currently getting OutofMemoryError looking for your contribution.

License
-------
This library is distributed under the LGPL.  
See: https://raw.github.com/RIPE-NCC/hadoop-pcap/master/LICENSE

Repository
----------

	<repositories>
	  <repository>
	    <id>hadoop-pcap</id>
	    <url>http://dl.bintray.com/hadoop-pcap/hadoop-pcap</url>
	  </repository>
	</repositories>


Screencast
----------

We have created a screencast showing the use of the Hadoop PCAP SerDe in Hive using [Amazon Elastic MapReduce](http://aws.amazon.com/elasticmapreduce/).  
You can find the video on YouTube: http://www.youtube.com/watch?v=FLxeQciax-Q


Components
----------

This project consists of two components:

### Library

Bundles the code used to read PCAPs. Can be used within MapReduce jobs to natively read PCAP files.  
See: https://github.com/RIPE-NCC/hadoop-pcap/tree/master/hadoop-pcap-lib

### SerDe

Implements a Hive Serializer/Deserializer (SerDe) to query PCAPs using SQL like commands.  
See: https://github.com/RIPE-NCC/hadoop-pcap/tree/master/hadoop-pcap-serde

=====================================================================================================================================
