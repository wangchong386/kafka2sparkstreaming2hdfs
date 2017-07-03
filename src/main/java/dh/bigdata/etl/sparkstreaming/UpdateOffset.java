package dh.bigdata.etl.sparkstreaming;

import dh.bigdata.etl.util.KafkaManager;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.util.HashMap;
import java.util.Map;

public class UpdateOffset implements Function2<JavaRDD<MessageAndMetadata<String, byte[]>>, Time, Void> {

    /**
     * 将偏移提交到zookeeper
     * @param rdd
     * @param time
     * @return
     * @throws Exception
     */
    @Override
    public Void call(JavaRDD<MessageAndMetadata<String, byte[]>> rdd, Time time) throws Exception {
        OffsetRange[] ranges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        Map<TopicAndPartition,Long> newOffsets = new HashMap<>(ranges.length);
        for (OffsetRange range : ranges) {
            newOffsets.put(new TopicAndPartition(range.topic(), range.partition()), range.untilOffset());
        }
        String zkOffsetManager = "d1-datanode35:2181";
        String group = "etl_dh_data";
        KafkaManager.setOffsets(zkOffsetManager, group, newOffsets);
        return null;
    }
}
