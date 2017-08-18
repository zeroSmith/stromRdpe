package com.bonc.storm.test;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;

public class Ds {
	public static Stream exec(TridentTopology topology) {
		// 设置zookeeper连接
		BrokerHosts brokerHosts = new ZkHosts("rdpe3:2181,rdpe4:2181,rdpe5:2181");
		// 设置kafka参数
		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts, "Source");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());// kafka数据解析
		kafkaConfig.fetchSizeBytes = 1048576 * 10;
		kafkaConfig.bufferSizeBytes = 1048576 * 10;
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(kafkaConfig);
		Stream stream = topology.newStream("read-kafka", spout);
		return stream;
	}
}
