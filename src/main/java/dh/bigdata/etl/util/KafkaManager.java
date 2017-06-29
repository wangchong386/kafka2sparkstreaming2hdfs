package dh.bigdata.etl.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import kafka.admin.AdminUtils;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaManager {
    private static final Logger log = LoggerFactory.getLogger(KafkaManager.class);
    private static final int ZK_TIMEOUT_MSEC = (int) TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);

    public static boolean topicExists(String zkServers, String topic) {
        try (AutoZkClient zkClient = new AutoZkClient(zkServers)) {
            return AdminUtils.topicExists(zkClient, topic);
        }
    }

    /*
     * zkOffsetManagement Zookeeper server string: host1:port1[,host2:port2,...]
     */
    public static void setOffsets(String zkOffsetManagement,
                                  String groupID,
                                  Map<TopicAndPartition, Long> offsets) {
        try (AutoZkClient zkClient = new AutoZkClient(zkOffsetManagement)) {
            for (Map.Entry<TopicAndPartition, Long> entry: offsets.entrySet()) {
                TopicAndPartition topicAndPartition = entry.getKey();
                Long offset = entry.getValue();
                ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupID, topicAndPartition.topic());
                int partition = topicAndPartition.partition();
                String partitionOffsetPath = topicDirs.consumerOffsetDir() + "/" + partition;
                ZkUtils.updatePersistentPath(zkClient, partitionOffsetPath, Long.toString(offset));
                log.info("updating offset path" + partitionOffsetPath + " offset=" + Long.toString(offset));
                System.out.println("updating offset path" + partitionOffsetPath + " offset=" + Long.toString(offset));
            }
        }
    }

    public static Map<TopicAndPartition,Long> getTopicOffsets(String zkServers, String topic){
        Map<TopicAndPartition,Long> retVals = new HashMap<TopicAndPartition,Long>();

        for(String zkServer:zkServers.split(",")){
            SimpleConsumer simpleConsumer = new SimpleConsumer(zkServer.split(":")[0],
                    Integer.valueOf(zkServer.split(":")[1]),
                    30000,
                    1024,
                    "consumer");
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Arrays.asList(topic));
            TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

            for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
                for (PartitionMetadata part : metadata.partitionsMetadata()) {
                    Broker leader = part.leader();
                    if (leader != null) {
                        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, part.partitionId());

                        PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 10000);
                        OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo), kafka.api.OffsetRequest.CurrentVersion(), simpleConsumer.clientId());
                        OffsetResponse offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest);

                        if (!offsetResponse.hasError()) {
                            long[] offsets = offsetResponse.offsets(topic, part.partitionId());
                            retVals.put(topicAndPartition, offsets[0]);
                        }
                    }
                }
            }
            simpleConsumer.close();
        }
        return retVals;
    }

    /*
     * zkKafkaServers Zookeeper server string: host1:port1[,host2:port2,...]
     */
    public static Map<TopicAndPartition, Long> getOffsets(String zkKafkaServers,
                                                          String zkOffSetManager,
                                                          String groupID,
                                                          String topic,
                                                          Map<String, String> kafkaParams) {
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupID, topic);
        Map<TopicAndPartition, Long> offsets = new HashMap<>();

        AutoZkClient zkKafkaClient = new AutoZkClient(zkKafkaServers);
        AutoZkClient zkOffsetManagerClient = new AutoZkClient(zkOffSetManager);

        List<Object> partitions = JavaConversions.seqAsJavaList(
                ZkUtils.getPartitionsForTopics(
                        zkKafkaClient,
                        JavaConversions.asScalaBuffer(Collections.singletonList(topic))).head()._2());
        for (Object partition : partitions) {
            String partitionOffsetPath = topicDirs.consumerOffsetDir() + "/" + partition;
            log.info("Offset location, zookeeper path=" + partitionOffsetPath);
            System.out.println("Offset location, zookeeper path=" + partitionOffsetPath);
            Option<String> maybeOffset = ZkUtils.readDataMaybeNull(zkOffsetManagerClient, partitionOffsetPath)._1();
            Long offset = maybeOffset.isDefined() ? Long.parseLong(maybeOffset.get()) : null;
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, Integer.parseInt(partition.toString()));
            offsets.put(topicAndPartition, offset);
        }

        fillInLatestOffsets(offsets, kafkaParams); // in case offsets are blank for any partition
        return offsets;
    }

    private static void fillInLatestOffsets(Map<TopicAndPartition, Long> offsets, Map<String, String> kafkaParams) {
        if (offsets.containsValue(null)) {
            Set<TopicAndPartition> needOffset = new HashSet<TopicAndPartition>();
            for(Map.Entry<TopicAndPartition, Long> entry:offsets.entrySet()){
                if(entry.getValue() == null){
                    needOffset.add(entry.getKey());
                }
            }
            log.info("No initial offsets for " + needOffset + " reading from Kafka");
            System.out.println("No initial offsets for " + needOffset + " reading from Kafka");

            // The high price of calling private Scala stuff:
            @SuppressWarnings("unchecked")
            scala.collection.immutable.Map<String, String> kafkaParamsScalaMap =
                    (scala.collection.immutable.Map<String, String>)
                            scala.collection.immutable.Map$.MODULE$.apply(JavaConversions.mapAsScalaMap(kafkaParams)
                                    .toSeq());
            @SuppressWarnings("unchecked")
            scala.collection.immutable.Set<TopicAndPartition> needOffsetScalaSet =
                    (scala.collection.immutable.Set<TopicAndPartition>)
                            scala.collection.immutable.Set$.MODULE$.apply(JavaConversions.asScalaSet(needOffset)
                                    .toSeq());

            KafkaCluster kc = new KafkaCluster(kafkaParamsScalaMap);
            Map<TopicAndPartition, ?> leaderOffsets =
                    JavaConversions.mapAsJavaMap(kc.getLatestLeaderOffsets(needOffsetScalaSet).right().get());
            for (Map.Entry<TopicAndPartition, ?> entry : leaderOffsets.entrySet()) {
                TopicAndPartition tAndP = entry.getKey();
                Object leaderOffsetsObj = entry.getValue();
                // Can't reference LeaderOffset class, so, hack away:
                Matcher m = Pattern.compile("LeaderOffset\\([^,]+,[^,]+,([^)]+)\\)").matcher(leaderOffsetsObj.toString());
                Preconditions.checkState(m.matches());
                offsets.put(tAndP, Long.valueOf(m.group(1)));
            }
        }
    }

    // Just exists for Closeable convenience
    private static final class AutoZkClient extends ZkClient implements Closeable {
        AutoZkClient(String zkServers) {
            super(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, ZKStringSerializer$.MODULE$);
        }
    }
}
