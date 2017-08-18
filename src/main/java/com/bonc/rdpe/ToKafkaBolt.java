package com.bonc.rdpe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.KafkaTopicSelector;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;


public class ToKafkaBolt<K,V> extends BaseRichBolt{
	private static final org.slf4j.Logger Log = LoggerFactory.getLogger(ToKafkaBolt.class);
    
    public static final String TOPIC = "topic";
    public static final String KAFKA_BROKER_PROPERTIES = "kafka.broker.properties";

    //private Producer<K, V> producer;
    private OutputCollector collector;
    private TupleToKafkaMapper<K, V> Mapper;
    private DefaultTopicSelector topicselector;
    
    public ToKafkaBolt<K,V> withTupleToKafkaMapper(TupleToKafkaMapper<K, V> mapper){
        this.Mapper = mapper;
        return this;
    }
    
    public ToKafkaBolt<K, V> withTopicSelector(DefaultTopicSelector topicSelector){
        this.topicselector = topicSelector;
        return this;
    }
    
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        
        if (Mapper == null) {
            this.Mapper = new FieldNameBasedTupleToKafkaMapper<K, V>();
        }
        
        if (topicselector == null) {
        	  this.topicselector = new DefaultTopicSelector((String)stormConf.get(TOPIC));
        }
        
        Map configMap = (Map) stormConf.get(KAFKA_BROKER_PROPERTIES);
       // Properties properties = new Properties();
      //  properties.putAll(configMap);
      //  ProducerConfig config = new ProducerConfig(properties);
     //   producer = new Producer<K, V>(config);
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String iString = input.getString(0);
        System.out.println(iString+"    从卡夫卡得到的数据");
        K key = null;//key 为null。
        V message = null;
        String topic = null;
        List<Object> list=new ArrayList<Object>();
        list.add("obj");
        collector.emit(new Values(list));
        
        try {
            
           /* key = Mapper.getKeyFromTuple(input);
            message = Mapper.getMessageFromTuple(input);
            topic = topicselector.getTopic(input);
            if (topic != null) {
                producer.send(new KeyedMessage<K, V>(topic,message));
                
            }else {
                Log.warn("skipping key = "+key+ ",topic selector returned null.");
            }*/
    
        } catch ( Exception e) {
            // TODO: handle exception
            //Log.error("Could not send message with key = " + key
                 //   + " and value = " + message + " to topic = " + topic, e);
        	e.printStackTrace();
        }finally{
            collector.ack(input);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    
    }

	
    

    
}