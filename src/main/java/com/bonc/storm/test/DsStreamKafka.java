package com.bonc.storm.test;
import kafka.api.OffsetRequest;

import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
public class DsStreamKafka {
	public  static  Stream  exec(TridentTopology topology){
		BrokerHosts brokerHosts = new ZkHosts("172.16.13.188:2181,172.16.13.189:2181,172.16.13.190:2181");
	    TridentKafkaConfig spoutConf = new TridentKafkaConfig(brokerHosts, "source", "");
	    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
	    //LatestTime：一次没读offset从头开始读取offset，读取过，会从增量的offset来读取
	    spoutConf.startOffsetTime = OffsetRequest.LatestTime();
	    spoutConf.useStartOffsetTimeIfOffsetOutOfRange = true;
	    spoutConf.fetchSizeBytes = 1024 * 1024 * 2;
	    spoutConf.bufferSizeBytes = 1024 * 1024 * 2;
		OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(spoutConf);
		Stream stream = topology.newStream("streamName", kafkaSpout).parallelismHint(2);
		return stream;
    }
}
