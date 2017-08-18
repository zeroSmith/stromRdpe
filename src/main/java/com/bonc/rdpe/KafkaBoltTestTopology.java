package com.bonc.rdpe;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TridentKafkaState;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Tuple;


public class KafkaBoltTestTopology {
    
    //配置kafka spout参数
    public static String kafka_zk_port = null;
    public static String topic = null;
    public static String kafka_zk_rootpath = null;
    public static BrokerHosts brokerHosts;
    public static String spout_name = "spout";
    public static String kafka_consume_from_start = null;
    
    public static class PrinterBolt extends BaseBasicBolt {

            private static final long serialVersionUID = 9114512339402566580L;
              public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }

           public void execute(Tuple tuple, BasicOutputCollector collector) {
                System.out.println("-----"+(tuple.getValue(1)).toString());
            }

        }
        
    public StormTopology buildTopology(){
        //kafkaspout 配置文件
        kafka_consume_from_start = "true";
        kafka_zk_rootpath = "/";
        String spout_id = spout_name;
        brokerHosts = new ZkHosts("node2:2181,node3:2181,node4:2181", "/brokers");
        kafka_zk_port = "2181";

        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, "fromkafka", kafka_zk_rootpath, spout_id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.zkPort = Integer.parseInt(kafka_zk_port);
        spoutConf.zkRoot = kafka_zk_rootpath;
        spoutConf.zkServers = Arrays.asList(new String[] {"node2", "node3", "node4"});
        
        //是否從kafka第一條數據開始讀取
        if (kafka_consume_from_start == null) {
            kafka_consume_from_start = "false";
        }
        boolean kafka_consume_frome_start_b = Boolean.valueOf(kafka_consume_from_start);
        if (kafka_consume_frome_start_b != true && kafka_consume_frome_start_b != false) {
            System.out.println("kafka_comsume_from_start must be true or false!");
        }
        System.out.println("kafka_consume_from_start: " + kafka_consume_frome_start_b);
   //     spoutConf.forceFromStart=kafka_consume_frome_start_b;
        
        TridentTopology topology=new TridentTopology();
        topology.newStream("txid", new KafkaSpout(spoutConf) );
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(spoutConf));
        builder.setBolt("forwardToKafka", new ToKafkaBolt<String, String>()).shuffleGrouping("spout");
        return builder.createTopology();
    }

    public static void main(String[] args) throws AuthorizationException  {
        
        KafkaBoltTestTopology kafkaBoltTestTopology = new KafkaBoltTestTopology();
        StormTopology stormTopology = kafkaBoltTestTopology.buildTopology();
        Hashtable<Object, Object> conf =  new  Hashtable<Object, Object>();
        /*//设置kafka producer的配置
        Properties props = new Properties();
        props.put("metadata.broker.list", "node3:9092");
        props.put("producer.type","async");
        props.put("request.required.acks", "0"); // 0 ,-1 ,1
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ((Map) conf).put("kafka.broker.properties", props);
        ((Hashtable<Object, Object>) conf).put("topic","testTokafka");*/

        if(args.length > 0){
            // cluster submit.
            try {
                 StormSubmitter.submitTopology("kafkaboltTest", (Map) conf, stormTopology);
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }else{
            new LocalCluster().submitTopology("kafkaboltTest", (Map) conf, stormTopology);
        }

    }
}