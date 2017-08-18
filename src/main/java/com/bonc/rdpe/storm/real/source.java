package com.bonc.rdpe.storm.real;

import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;

 

public  class   source{
	
	public  static  Stream  exec(TridentTopology topology){
		BrokerHosts brokerHosts = new ZkHosts("${inZks}");
	    TridentKafkaConfig spoutConf = new TridentKafkaConfig(brokerHosts, "${inTopic}", "${groupId}");
	    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
	    //LatestTime：一次没读offset从头开始读取offset，读取过，会从增量的offset来读取
	    spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
	    spoutConf.useStartOffsetTimeIfOffsetOutOfRange = true;
	    spoutConf.fetchSizeBytes = 1024 * 1024 * 2;
	    spoutConf.bufferSizeBytes = 1024 * 1024 * 2;
		OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(new TridentKafkaConfig((BrokerHosts) new Broker("brokerHost"), "${}"));
		Stream stream = topology.newStream("${streamName}", kafkaSpout).parallelismHint(8);
		return stream;
	}
	
	/*  public static TridentKafkaConfig getKafkaConfig() {
		    BrokerHosts brokerHosts = new ZkHosts("${inZks}");
		    TridentKafkaConfig spoutConf = new TridentKafkaConfig(brokerHosts, "${inTopic}", "${groupId}");
		    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		    //LatestTime：一次没读offset从头开始读取offset，读取过，会从增量的offset来读取
		      spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		      spoutConf.useStartOffsetTimeIfOffsetOutOfRange = true;
		      spoutConf.fetchSizeBytes = 1024 * 1024 * 2;
		      spoutConf.bufferSizeBytes = 1024 * 1024 * 2;
		    return spoutConf;
		  }
	
	
	
	///
	public static  Stream exec(TridentTopology topology){
        String kafka_zk_rootpath = "/";
        String spout_id = "${spoutname}";
        BrokerHosts  brokerHosts = new ZkHosts("${zkIP:port}");
        String  kafka_zk_port = "${zk_port}";
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, "${topicName}", kafka_zk_rootpath, spout_id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.zkPort = 2181;
        spoutConf.zkRoot = kafka_zk_rootpath;
        spoutConf.zkServers = Arrays.asList(new String[] {"ip"}); //zknodeIP需要遍历
        Stream stream= topology.newStream("txid", new KafkaSpout(spoutConf) );
        return stream;
    }*/
 }