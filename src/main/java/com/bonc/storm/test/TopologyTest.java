package com.bonc.storm.test;

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;


public class TopologyTest {
	public static void main(String args[]){
		Stream stream=new DsStreamKafka().exec(new TridentTopology());	
		Stream stream1=new FilterStream().exec(stream);
		Stream steam2=new ParseStream().exec(stream1);
	
	}

}
