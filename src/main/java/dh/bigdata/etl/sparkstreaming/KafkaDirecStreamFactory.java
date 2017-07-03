package dh.bigdata.etl.sparkstreaming;

import dh.bigdata.etl.util.Functions;
import dh.bigdata.etl.util.KafkaManager;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KafkaDirecStreamFactory {




    public static JavaInputDStream<MessageAndMetadata<String, byte[]>> get(JavaStreamingContext jssc, String topic) {
        String zkKafkaServers = "d1-datanode35:2181";
        String zkOffSetManager = zkKafkaServers;
        String groupID = "etl_dh_data";
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "d1-kafka1:9092,d1-kafka2:9092");
        kafkaParams.put("auto.offset.reset", "smallest");
        kafkaParams.put("group.id", "etl_dh_data");
        Map<TopicAndPartition, Long> topicOffsets = KafkaManager.getTopicOffsets("d1-kafka1:9092,d1-kafka2:9092", topic);
        Map<TopicAndPartition, Long> consumerOffsets = KafkaManager.getOffsets(zkKafkaServers, zkOffSetManager, groupID, topic, kafkaParams);
        if(null!=consumerOffsets && consumerOffsets.size()>0){
            topicOffsets.putAll(consumerOffsets);
        }
        for(Map.Entry<TopicAndPartition,Long> entry:topicOffsets.entrySet()){
            System.out.println(entry.getKey().topic()+"\t"+entry.getKey().partition()+"\t"+entry.getValue());
        }

        Map<String,String> odsLogRecsKafkaParameters = new HashMap<>();
        odsLogRecsKafkaParameters.put("metadata.broker.list", "d1-kafka1:9092,d1-kafka2:9092");
        odsLogRecsKafkaParameters.put("auto.offset.reset", "smallest");
        odsLogRecsKafkaParameters.put("group.id", "etl_dh_data");
        Set<String> odsLogRecsTopics =new HashSet<>();
        odsLogRecsTopics.add(topic);

        @SuppressWarnings("unchecked")
        Class<MessageAndMetadata<String, byte[]>> streamClass =
                (Class<MessageAndMetadata<String, byte[]>>) (Class<?>) MessageAndMetadata.class;

        JavaInputDStream<MessageAndMetadata<String, byte[]>> odsLogRecsKafkaRdd = KafkaUtils.createDirectStream(jssc,
                String.class,
                byte[].class,
                StringDecoder.class,
                DefaultDecoder.class,
                streamClass,
                odsLogRecsKafkaParameters,
                topicOffsets,
                Functions.<MessageAndMetadata<String, byte[]>>identity());
        return odsLogRecsKafkaRdd;
    }
}
