package dh.bigdata.etl.sparkstreaming;

import dh.bigdata.etl.util.ConfUtil;
import dh.bigdata.etl.util.Functions;
import dh.bigdata.etl.util.KafkaManager;
import dh.bigdata.etl.util.Contant;
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
        ConfUtil confUtil = ConfUtil.getInstance();
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", confUtil.getProperty(Contant.KAFKA_CLUSTOR));
        kafkaParams.put("auto.offset.reset", confUtil.getProperty(Contant.AUTO_OFFSET_RESET));
        kafkaParams.put("group.id", confUtil.getProperty(Contant.GROUP_ID));

        Map<TopicAndPartition, Long> topicOffsets = KafkaManager.getTopicOffsets(confUtil.getProperty(Contant.KAFKA_CLUSTOR), topic);
        Map<TopicAndPartition, Long> consumerOffsets = KafkaManager.getOffsets(confUtil.getProperty(Contant.ZK_SERVERS),
                confUtil.getProperty(Contant.ZK_SERVERS),
                confUtil.getProperty(Contant.GROUP_ID),
                topic,
                kafkaParams);
        if(null!=consumerOffsets && consumerOffsets.size()>0){
            topicOffsets.putAll(consumerOffsets);
        }
        for(Map.Entry<TopicAndPartition,Long> entry:topicOffsets.entrySet()){
            System.out.println(entry.getKey().topic()+"\t"+entry.getKey().partition()+"\t"+entry.getValue());
        }

        @SuppressWarnings("unchecked")
        Class<MessageAndMetadata<String, byte[]>> streamClass =
                (Class<MessageAndMetadata<String, byte[]>>) (Class<?>) MessageAndMetadata.class;

        JavaInputDStream<MessageAndMetadata<String, byte[]>> kafkaRdd = KafkaUtils.createDirectStream(jssc,
                String.class,
                byte[].class,
                StringDecoder.class,
                DefaultDecoder.class,
                streamClass,
                kafkaParams,
                topicOffsets,
                Functions.<MessageAndMetadata<String, byte[]>>identity());
        return kafkaRdd;
    }
}
