package dh.bigdata.etl;

import com.dhgate.event.DHEvent;
import dh.bigdata.etl.util.Functions;
import dh.bigdata.etl.util.KafkaManager;
import dh.bigdata.etl.util.PigConv;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.util.*;

public class Main {

    public static volatile Broadcast<HiveContext> broadcastHC = null;

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("etl_dh_data").setMaster("yarn-client");
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));
        //jssc.checkpoint("");

        HiveContext hiveContext = new HiveContext(jssc.sparkContext().sc());
        hiveContext.setConf("hive.execution.engine","spark");
        broadcastHC = jssc.sc().broadcast(hiveContext);

               /*
         * Search_S0005 -> tmp_ods_log_rec_s -> ods_log_rec_s
         * */
        /* get offset from zookeeper */
        String zkKafkaServers = "d1-datanode35:2181";
        String zkOffSetManager = zkKafkaServers;
        String groupID = "etl_dh_data";
        String topic = "ods_log_rec_s";
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
        odsLogRecsTopics.add("ods_log_rec_s");

        @SuppressWarnings("unchecked")
        Class<MessageAndMetadata<String, String>> streamClass =
                (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;

        JavaInputDStream<MessageAndMetadata<String, String>> odsLogRecsKafkaRdd = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                streamClass,
                odsLogRecsKafkaParameters,
                topicOffsets,
                Functions.<MessageAndMetadata<String, String>>identity());
        odsLogRecsKafkaRdd.print(10);
        //odsLogRecsKafkaRdd.persist();

        /* 蒋kafka中数据转换成DHEvent */
        JavaDStream<DHEvent> odsLogRecsEventRdd = odsLogRecsKafkaRdd.map(new Function<MessageAndMetadata<String,String>, DHEvent>() {

            @Override
            public DHEvent call(MessageAndMetadata<String,String> message) throws Exception {
                String messageSum = message.message();
                String eventBody = messageSum.substring(messageSum.indexOf("Search_S0005"));
                DHEvent event = DHEvent.parse(eventBody);

                String eventHeader = messageSum.substring(0, messageSum.indexOf("Search_S0005"));
                int startIndex = eventHeader.indexOf("timestamp") + "timestamp".length();
                String curTimeMs = eventHeader.substring(startIndex + 1, startIndex + 1 + 13); // "149821365588"
                java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String curTime = formatter.format(new Date(Long.parseLong(curTimeMs)));
                String curDate = curTime.substring(0, "yyyy-MM-dd".length());
                event.getTags().put("currentDate", curDate);
                return event;
            }
        });

        /* 将event对应转换成tmp_ods_log_rec_s中的记录 */
        JavaDStream<Row> odsLogRecsRowRdd = odsLogRecsEventRdd.map(new Function<DHEvent, Row>() {

            @Override
            public Row call(DHEvent event) throws Exception {
                String site = null;
                if (event.getTags().get("site") != null && !"".equals(event.getTags().get("site").toString())) {
                    site = event.getTags().get("site").toString();
                } else {
                    String siteTmp = PigConv.getSitebyUrl(event.getU());
                    if (siteTmp == null || "".equals(siteTmp)) {
                        site = "www";
                    } else {
                        site = siteTmp;
                    }
                }
                String lang = null;
                if (event.getTags().get("lang") != null && !"".equals(event.getTags().get("lang").toString())) {
                    lang = event.getTags().get("lang").toString();
                } else {
                    String langTmp = PigConv.getLangbyUrl(event.getU());
                    if (langTmp == null || "".equals(langTmp)) {
                        lang = "en";
                    } else {
                        lang = langTmp;
                    }
                }
                return RowFactory.create(event.getName(),
                        event.getId(),
                        event.getVid(),
                        event.getUsrid(),
                        event.getSid(),
                        event.getVt(),
                        event.getIp(),
                        event.getUa(),
                        event.getF(),
                        event.getD(),
                        event.getRefurl(),
                        event.getUlevel(),
                        event.getAid(),
                        event.getU(),
                        event.getCou(),
                        (event.getTags().get("uuid") == null) ? "" : event.getTags().get("uuid").toString(),
                        (event.getTags().get("cid") == null) ? "" : event.getTags().get("cid").toString(),
                        site,
                        lang,
                        (event.getTags().get("operation_system") == null) ? "" : event.getTags().get("operation_system").toString(),
                        (event.getTags().get("browser_version") == null) ? "" : event.getTags().get("browser_version").toString(),
                        (event.getTags().get("imploc") == null) ? "" : event.getTags().get("imploc").toString(),
                        (event.getTags().get("browser") == null) ? "" : event.getTags().get("browser").toString(),
                        (event.getTags().get("pic") == null) ? "" : event.getTags().get("pic").toString(),
                        (event.getTags().get("algo") == null) ? "" : event.getTags().get("algo").toString(),
                        (event.getTags().get("cytpe") == null) ? "" : event.getTags().get("cytpe").toString(),
                        event.getTags().get("currentDate").toString());
            }
        });

        System.out.println("odsLogRecsRowRdd = ");
        odsLogRecsRowRdd.print(10);

        odsLogRecsRowRdd.foreachRDD(new Function2<JavaRDD<Row>, Time, Void>() {

            @Override
            public Void call(JavaRDD<Row> rdd, Time time) throws Exception {
                if (!rdd.isEmpty()) {
                    List<StructField> structFields = new ArrayList<>();
                    structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
                    structFields.add(DataTypes.createStructField("vid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("usrid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("sid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("vt", DataTypes.LongType, true));
                    structFields.add(DataTypes.createStructField("ip", DataTypes.LongType, true));
                    structFields.add(DataTypes.createStructField("ua", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("f", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("d", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("refurl", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ulevel", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("aid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("u", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("cou", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("uuid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("cid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("site", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("operation_system", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("browser_version", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("imploc", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("browser", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pic", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("algo", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("cytpe", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("currentDate", DataTypes.StringType, true));
                    StructType structType = DataTypes.createStructType(structFields);

                    HiveContext hiveContext = broadcastHC.value();
                    DataFrame df = hiveContext.createDataFrame(rdd, structType);
                    df.registerTempTable("tmp_ods_log_rec_s");

                    hiveContext.sql("set hive.exec.dynamic.partition=true");
                    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
                    hiveContext.sql("INSERT INTO sparkstreaming.tmp_ods_log_rec_s PARTITION (dt) select * from tmp_ods_log_rec_s");
                }
                return null;
            }
        });

        odsLogRecsKafkaRdd.foreachRDD(new Function2<JavaRDD<MessageAndMetadata<String, String>>, Time, Void>() {

            @Override
            public Void call(JavaRDD<MessageAndMetadata<String, String>> rdd, Time time) throws Exception {
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
        });

        /*
         * //Item_U0003 -> tmp_ods_log_pageview -> ods_log_pageview
         * Public_S0003|Search_U0001|Item_U0001|Checkout_U0001|Item_U0003 -> tmp_ods_log_pageview -> ods_log_pageview
         * */
        /* get offset from zookeeper */
        String zkKafkaServers = "d1-datanode35:2181";
        String zkOffSetManager = zkKafkaServers;
        String groupID = "etl_dh_data";
        String topic = "ods_log_pageview";
        Map<String,String> kafkaParams = new HashMap<>();
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
        Map<String,String> odsLogPageviewKafkaParameters = new HashMap<>();
        odsLogPageviewKafkaParameters.put("metadata.broker.list", "d1-kafka1:9092,d1-kafka2:9092");
        odsLogPageviewKafkaParameters.put("auto.offset.reset", "smallest");
        odsLogPageviewKafkaParameters.put("group.id", "etl_dh_data");
        Set<String> odsLogPageviewTopics =new HashSet<>();
        odsLogPageviewTopics.add("ods_log_pageview");
        @SuppressWarnings("unchecked")
        Class<MessageAndMetadata<String, String>> streamClass =
                (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;
        JavaInputDStream<MessageAndMetadata<String, String>> odsLogPageviewKafkaRdd = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                streamClass,
                odsLogRecsKafkaParameters,
                topicOffsets,
                Functions.<MessageAndMetadata<String, String>>identity());
        odsLogPageviewKafkaRdd.print(10);
        //odsLogRecsKafkaRdd.persist();

        /* 将kafka中数据转换成DHEvent */
        JavaDStream<DHEvent> odsLogPageviewEventRdd = odsLogPageviewKafkaRdd.map(new Function<MessageAndMetadata<String,String>, DHEvent>() {

            @Override
            public DHEvent call(MessageAndMetadata<String,String> message) throws Exception {
                //String eventBody = tuple._2..matches"Public_S0003|Search_U0001|Item_U0001|Checkout_U0001|Item_U0003;
                String messageSum = message.message();
                String eventBody = messageSum.substring(messageSum.indexOf("Item_U0003"));
                DHEvent event = DHEvent.parse(eventBody);
                String eventHeader = messageSum.substring(0, messageSum.indexOf("Item_U0003"));
                int startIndex = eventHeader.indexOf("timestamp") + "timestamp".length();
                String curTimeMs = eventHeader.substring(startIndex + 1, startIndex + 1 + 13); // "149821365588"
                java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String curTime = formatter.format(new Date(Long.parseLong(curTimeMs)));
                String curDate = curTime.substring(0, "yyyy-MM-dd".length());
                event.getTags().put("currentDate", curDate);
                return event;
            }
        });

        /* 将event对应转换成tmp_ods_log_pageview中的记录 */
        JavaDStream<Row> odsLogPageviewRowRdd = odsLogPageviewEventRdd.map(new Function<DHEvent, Row>() {

            @Override
            public Row call(DHEvent event) throws Exception {

                String site = null;
                if (event.getTags().get("site") != null && !"".equals(event.getTags().get("site").toString())) {
                    site = event.getTags().get("site").toString();
                } else {
                    String siteTmp = PigConv.getSitebyUrl(event.getU());
                    if (siteTmp == null || "".equals(siteTmp)) {
                        site = "www";
                    } else {
                        site = siteTmp;
                    }
                }
                String lang = null;
                if (event.getTags().get("lang") != null && !"".equals(event.getTags().get("lang").toString())) {
                    lang = event.getTags().get("lang").toString();
                } else {
                    String langTmp = PigConv.getLangbyUrl(event.getU());
                    if (langTmp == null || "".equals(langTmp)) {
                        lang = "en";
                    } else {
                        lang = langTmp;
                    }
                }
                String uuid = null;
                if (event.getTags().get("lang") != null && !"".equals(event.getTags().get("lang").toString())) {
                    lang = event.getTags().get("lang").toString();
                } else {
                    String langTmp = PigConv.getLangbyUrl(event.getU());
                    if (langTmp == null || "".equals(langTmp)) {
                        lang = "en";
                    } else {
                        lang = langTmp;
                    }
                }
                String uuid = PigConv.getUuid(event.gettype());
                return RowFactory.create(event.getName(),
                        event.getId(),
                        event.getVid(),
                        event.getUsrid(),
                        event.getSid(),
                        event.getVt(),
                        event.getIp(),
                        event.getUa(),
                        event.getF(),
                        event.getD(),
                        event.getRefurl(),
                        event.getUlevel(),
                        event.getAid(),
                        event.getU(),
                        event.getCou(),
                        (event.getTags().get("uuid") == null) ? "" : event.getTags().get("uuid").toString(),
                        (event.getTags().get("pt") == null) ? "" : event.getTags().get("pt").toString(),
                        (event.getTags().get("lpid") == null) ? "" : event.getTags().get("lpid").toString(),
                        (event.getTags().get("activity") == null) ? "" : event.getTags().get("activity").toString(),
                        (event.getTags().get("supplierid") == null) ? "" : event.getTags().get("supplierid").toString(),
                        (event.getTags().get("pos") == null) ? "" : event.getTags().get("pos").toString(),
                        (event.getTags().get("clkloc") == null) ? "" : event.getTags().get("clkloc").toString(),
                        (event.getTags().get("cid") == null) ? "" : event.getTags().get("cid").toString(),
                        (event.getTags().get("catepubid") == null) ? "" : event.getTags().get("catepubid").toString(),
                        (event.getTags().get("picl") == null) ? "" : event.getTags().get("picl").toString(),
                        (event.getTags().get("pnl") == null) ? "" : event.getTags().get("pnl").toString(),
                        (event.getTags().get("spcst") == null) ? "" : event.getTags().get("spcst").toString(),
                        (event.getTags().get("wdc") == null) ? "" : event.getTags().get("wdc").toString(),
                        (event.getTags().get("rnl") == null) ? "" : event.getTags().get("rnl").toString(),
                        (event.getTags().get("ttp") == null) ? "" : event.getTags().get("ttp").toString(),
                        (event.getTags().get("imploc") == null) ? "" : event.getTags().get("imploc").toString(),
                        site,
                        lang,
                        (event.getTags().get("ordlang") == null) ? "" : event.getTags().get("ordlang").toString(),
                        (event.getTags().get("operation_system") == null) ? "" : event.getTags().get("operation_system").toString(),
                        (event.getTags().get("version") == null) ? "" : event.getTags().get("version").toString(),
                        (event.getTags().get("lastvisittime") == null) ? "" : event.getTags().get("lastvisittime").toString(),
                        (event.getTags().get("pvn") == null) ? "" : event.getTags().get("pvn").toString(),
                        (event.getTags().get("vnum") == null) ? "" : event.getTags().get("vnum").toString(),
                        (event.getTags().get("pagedur") == null) ? "" : event.getTags().get("pagedur").toString(),
                        (event.getTags().get("session") == null) ? "" : event.getTags().get("session").toString(),
                        (event.getTags().get("subpt") == null) ? "" : event.getTags().get("subpt").toString(),
                        event.getTags().get("currentDate").toString());
            }
        });

        System.out.println("odsLogPageviewRowRdd = ");
        odsLogPageviewRowRdd.print(10);

        odsLogPageviewRowRdd.foreachRDD(new Function2<JavaRDD<Row>, Time, Void>() {

            @Override
            public Void call(JavaRDD<Row> rdd, Time time) throws Exception {
                if (!rdd.isEmpty()) {
                    List<StructField> structFields = new ArrayList<>();
                    structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
                    structFields.add(DataTypes.createStructField("vid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("usrid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("sid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("vt", DataTypes.LongType, true));
                    structFields.add(DataTypes.createStructField("ip", DataTypes.LongType, true));
                    structFields.add(DataTypes.createStructField("ua", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("f", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("d", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("refurl", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ulevel", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("aid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("u", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("cou", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("uuid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pt", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("lpid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("activity", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("supplierid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pic", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pos", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("clkloc", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("cid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("catepubid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("picl", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pril", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pnl", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("spcst", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("wdc", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("rnl", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ttp", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("imploc", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("site", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ordlang", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("operation_system", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("version", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("lastvisittime", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pvn", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("vnum", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pagedur", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("session", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("subpt", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("currentDate", DataTypes.StringType, true));
                    StructType structType = DataTypes.createStructType(structFields);

                    HiveContext hiveContext = broadcastHC.value();
                    DataFrame df = hiveContext.createDataFrame(rdd, structType);
                    df.registerTempTable("tmp_ods_log_pageview");

                    hiveContext.sql("set hive.exec.dynamic.partition=true");
                    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
                    hiveContext.sql("INSERT INTO sparkstreaming.tmp_ods_log_pageview PARTITION (dt) select * from tmp_ods_log_pageview");
                }
                return null;
            }
        });
        odsLogPageviewKafkaRdd.foreachRDD(new Function2<JavaRDD<MessageAndMetadata<String, String>>, Time, Void>() {

            @Override
            public Void call(JavaRDD<MessageAndMetadata<String, String>> rdd, Time time) throws Exception {
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
        });
         /*
         * Search_S0001 -> tmp_ods_log_search_s -> ods_log_search_s
         */
         /* get offset from zookeeper */
        String zkKafkaServers = "d1-datanode35:2181";
        String zkOffSetManager = zkKafkaServers;
        String groupID = "etl_dh_data";
        String topic = "ods_log_search_s";
        Map<String,String> kafkaParams = new HashMap<>();
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
        Map<String,String> odsLogSearchsKafkaParameters = new HashMap<>();
        odsLogSearchsKafkaParameters.put("metadata.broker.list", "d1-kafka1:9092,d1-kafka2:9092");
        odsLogSearchsKafkaParameters.put("auto.offset.reset", "smallest");
        odsLogSearchsKafkaParameters.put("group.id", "etl_dh_data");
        Set<String> odsLogSearchsTopics =new HashSet<>();
        odsLogSearchsTopics.add("ods_log_search_s");
        @SuppressWarnings("unchecked")
        Class<MessageAndMetadata<String, String>> streamClass =
                (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;

        JavaInputDStream<MessageAndMetadata<String, String>> odsLogSearchsKafkaRdd = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                odsLogSearchsKafkaParameters,
                topicOffsets,
                Functions.<MessageAndMetadata<String, String>>identity());

        odsLogSearchsKafkaRdd.print(10);
        //odsLogSearchsKafkaRdd.persist();
        /* 将kafka中数据转换成DHEvent */
        JavaDStream<DHEvent> odsLogSearchsEventRdd = odsLogSearchsKafkaRdd.map(new Function<MessageAndMetadata<String,String>, DHEvent>() {

            @Override
            public DHEvent call(MessageAndMetadata<String,String> message)  throws Exception {
                String messageSum = message.message();
                String eventBody = messageSum.substring(messageSum.indexOf("Search_S0001"));
                DHEvent event = DHEvent.parse(eventBody);

                String eventHeader = messageSum.substring(0, messageSum.indexOf("Search_S0001"));
                int startIndex = eventHeader.indexOf("timestamp") + "timestamp".length();
                String curTimeMs = eventHeader.substring(startIndex + 1, startIndex + 1 + 13); // "149821365588"
                java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String curTime = formatter.format(new Date(Long.parseLong(curTimeMs)));
                String curDate = curTime.substring(0, "yyyy-MM-dd".length());
                event.getTags().put("currentDate", curDate);
                return event;
            }
        });
        /* 将event对应转换成tmp_ods_log_search_s中的记录 */
        JavaDStream<Row> odsLogSearchsRowRdd = odsLogSearchsEventRdd.map(new Function<DHEvent, Row>() {
            @Override
            public Row call(DHEvent event) throws Exception {
                String site = null;
                if (event.getTags().get("site") != null && !"".equals(event.getTags().get("site").toString())) {
                    site = event.getTags().get("site").toString();
                } else {
                    String siteTmp = PigConv.getSitebyUrl(event.getU());
                    if (siteTmp == null || "".equals(siteTmp)) {
                        site = "www";
                    } else {
                        site = siteTmp;
                    }
                }
                String lang = null;
                if (event.getTags().get("lang") != null && !"".equals(event.getTags().get("lang").toString())) {
                    lang = event.getTags().get("lang").toString();
                } else {
                    String langTmp = PigConv.getLangbyUrl(event.getU());
                    if (langTmp == null || "".equals(langTmp)) {
                        lang = "en";
                    } else {
                        lang = langTmp;
                    }
                }
                String Schkw = null;
                Schkw = event.getTags().get("Schkw").toString();
                String search_keywords_md5 = PigConv.SchkwMD5(event.getSchkw());

                return RowFactory.create(event.getName(),
                        event.getId(),
                        event.getVid(),
                        event.getUsrid(),
                        event.getSid(),
                        event.getVt(),
                        event.getIp(),
                        event.getUa(),
                        event.getF(),
                        event.getD(),
                        event.getRefurl(),
                        event.getUlevel(),
                        event.getAid(),
                        event.getU(),
                        event.getCou(),
                        (event.getTags().get("uuid") == null) ? "" : event.getTags().get("uuid").toString(),
                        (event.getTags().get("schkw") == null) ? "" : event.getTags().get("schkw").toString(),
                        (event.getTags().get("schtype") == null) ? "" : event.getTags().get("schtype").toString(),
                        (event.getTags().get("browser_version") == null) ? "" : event.getTags().get("browser_version").toString(),
                        (event.getTags().get("dcid") == null) ? "" : event.getTags().get("dcid").toString(),
                        (event.getTags().get("cid") == null) ? "" : event.getTags().get("cid").toString(),
                        (event.getTags().get("recomdcids") == null) ? "" : event.getTags().get("recomdcids").toString(),
                        (event.getTags().get("cids") == null) ? "" : event.getTags().get("cids").toString(),
                        (event.getTags().get("ass") == null) ? "" : event.getTags().get("ass").toString(),
                        (event.getTags().get("sorttype") == null) ? "" : event.getTags().get("sorttype").toString(),
                        (event.getTags().get("rsnum") == null) ? "" : event.getTags().get("rsnum").toString(),
                        (event.getTags().get("pagenum") == null) ? "" : event.getTags().get("pagenum").toString(),
                        (event.getTags().get("page") == null) ? "" : event.getTags().get("page").toString(),
                        (event.getTags().get("itemnum") == null) ? "" : event.getTags().get("itemnum").toString(),
                        (event.getTags().get("relsch") == null) ? "" : event.getTags().get("relsch").toString(),
                        (event.getTags().get("ft_vipdiscount") == null) ? "" : event.getTags().get("ft_vipdiscount").toString(),
                        (event.getTags().get("ft_customerreview") == null) ? "" : event.getTags().get("ft_customerreview").toString(),
                        (event.getTags().get("ft_minprice") == null) ? "" : event.getTags().get("ft_minprice").toString(),
                        (event.getTags().get("ft_maxprice") == null) ? "" : event.getTags().get("ft_maxprice").toString(),
                        site,
                        lang,
                        (event.getTags().get("ordlang") == null) ? "" : event.getTags().get("ordlang").toString(),
                        (event.getTags().get("operation_system") == null) ? "" : event.getTags().get("operation_system").toString(),
                      //  (event.getTags().get("schkw") == null) ? "" : event.getTags().get("search_keywords_md5").toString(),
                        (event.getTags().get("abtest") == null) ? "" : event.getTags().get("abtest").toString(),
                        (event.getTags().get("ft_dispro") == null) ? "" : event.getTags().get("ft_dispro").toString(),
                        (event.getTags().get("ft_ispromotion") == null) ? "" : event.getTags().get("ft_ispromotion").toString(),
                        (event.getTags().get("ft_oneday") == null) ? "" : event.getTags().get("ft_oneday").toString(),
                        (event.getTags().get("ft_sevenreturn") == null) ? "" : event.getTags().get("ft_sevenreturn").toString(),
                        (event.getTags().get("ft_wholesaleonly") == null) ? "" : event.getTags().get("ft_wholesaleonly").toString(),
                        (event.getTags().get("ft_minorder") == null) ? "" : event.getTags().get("ft_minorder").toString(),
                        (event.getTags().get("ft_freeshipping") == null) ? "" : event.getTags().get("ft_freeshipping").toString(),
                        (event.getTags().get("ft_selleronline") == null) ? "" : event.getTags().get("ft_selleronline").toString(),
                        (event.getTags().get("ft_descpledge") == null) ? "" : event.getTags().get("ft_descpledge").toString(),
                        (event.getTags().get("gallery") == null) ? "" : event.getTags().get("gallery").toString(),
                        (event.getTags().get("ft_shipall") == null) ? "" : event.getTags().get("ft_shipall").toString(),
                        (event.getTags().get("ft_shipmethodnote") == null) ? "" : event.getTags().get("ft_shipmethodnote").toString(),
                        (event.getTags().get("ft_shipcountry") == null) ? "" : event.getTags().get("ft_shipcountry").toString(),
                        (event.getTags().get("ft_shipCountryName") == null) ? "" : event.getTags().get("ft_shipCountryName").toString(),
                        (event.getTags().get("totalPro") == null) ? "" : event.getTags().get("totalPro").toString(),
                        (event.getTags().get("crumbs") == null) ? "" : event.getTags().get("crumbs").toString(),
                        (event.getTags().get("ft_singleonly") == null) ? "" : event.getTags().get("ft_singleonly").toString(),
                        (event.getTags().get("algo") == null) ? "" : event.getTags().get("algo").toString(),
                        (event.getTags().get("ft_sponsor") == null) ? "" : event.getTags().get("ft_sponsor").toString()
                );
            }
        });
        System.out.println("odsLogSearchsRowRdd = ");
        odsLogSearchsRowRdd.print();

        odsLogSearchsRowRdd.foreachRDD(new Function2<JavaRDD<Row>, Time, Void>() {

            @Override
            public Void call(JavaRDD<Row> rdd, Time time) throws Exception {
                if (!rdd.isEmpty()) {
                    List<StructField> structFields = new ArrayList<>();
                    structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
                    structFields.add(DataTypes.createStructField("vid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("usrid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("sid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("vt", DataTypes.LongType, true));
                    structFields.add(DataTypes.createStructField("ip", DataTypes.LongType, true));
                    structFields.add(DataTypes.createStructField("ua", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("f", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("d", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("refurl", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ulevel", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("aid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("u", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("cou", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("uuid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("schkw", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("schtype", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("dcid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("cid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("recomdcids", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("cids", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ass", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("sorttype", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("rsnum", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pagenum", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("page", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("itemnum", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("relsch", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_vipdiscount", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_customerreview", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_minprice", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_maxprice", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("site", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ordlang", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("operation_system", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("search_keywords_md5", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("abtest", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_dispro", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_ispromotion", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_oneday", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_sevenreturn", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_wholesaleonly", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_minorder", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_freeshipping", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_selleronline", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_descpledge", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("gallery", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_shipall", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_shipmethodnote", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_shipcountry", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_shipCountryName", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("totalPro", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("crumbs", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_singleonly", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("algo", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ft_sponsor", DataTypes.StringType, true));
                    StructType structType = DataTypes.createStructType(structFields);

                    HiveContext hiveContext = broadcastHC.value();
                    DataFrame df = hiveContext.createDataFrame(rdd, structType);
                    df.registerTempTable("tmp_ods_log_search_s");

                    hiveContext.sql("set hive.exec.dynamic.partition=true");
                    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
                    hiveContext.sql("insert into sparkstreaming.tmp_ods_log_search_s partition(dt) select * from tmp_ods_log_search_s");
                }
                return null;
            }
        });
        odsLogSearchsKafkaRdd.foreachRDD(new Function2<JavaRDD<MessageAndMetadata<String, String>>, Time, Void>() {

            @Override
            public Void call(JavaRDD<MessageAndMetadata<String, String>> rdd, Time time) throws Exception {
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
        });
         /*
         * Public_S0001 -> tmp_ods_log_prod_expo -> ods_log_prod_expo
         */
         /* get offset from zookeeper */
        String zkKafkaServers = "d1-datanode35:2181";
        String zkOffSetManager = zkKafkaServers;
        String groupID = "etl_dh_data";
        String topic = "ods_log_prod_expo";
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
        Map<String,String> odsLogProdExpoKafkaParameters = new HashMap<>();
        odsLogProdExpoKafkaParameters.put("metadata.broker.list", "d1-kafka1:9092,d1-kafka2:9092");
        odsLogProdExpoKafkaParameters.put("auto.offset.reset", "smallest");
        odsLogProdExpoKafkaParameters.put("group.id", "etl_dh_data");
        Set<String> odsLogProdExpoTopics =new HashSet<>();
        odsLogProdExpoTopics.add("ods_log_prod_expo");
        @SuppressWarnings("unchecked")
        Class<MessageAndMetadata<String, String>> streamClass =
                (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;
        JavaInputDStream<MessageAndMetadata<String, String>> odsLogProdExpoKafkaRdd = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                odsLogProdExpoKafkaParameters,
                topicOffsets,
                Functions.<MessageAndMetadata<String, String>>identity());

        odsLogProdExpoKafkaRdd.print(10);
        //odsLogProdExpoKafkaRdd.persist();
        /* 将kafka中数据转换成DHEvent */
        JavaDStream<DHEvent> odsLogProdExpoEventRdd = odsLogProdExpoKafkaRdd.map(new Function<MessageAndMetadata<String,String>, DHEvent>() {

            @Override
            public DHEvent call(MessageAndMetadata<String,String> message) throws Exception {
                String messageSum = message.message();
                String eventBody = messageSum.substring(messageSum.indexOf("Public_S0001"));
                String eventHeader = messageSum.substring(0, messageSum.indexOf("Public_S0001"));
                int startIndex = eventHeader.indexOf("timestamp") + "timestamp".length();
                String curTimeMs = eventHeader.substring(startIndex + 1, startIndex + 1 + 13); // "149821365588"
                java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String curTime = formatter.format(new Date(Long.parseLong(curTimeMs)));
                String curDate = curTime.substring(0, "yyyy-MM-dd".length());
                event.getTags().put("currentDate", curDate);
                return event;
            }
        });
        /* 将event对应转换成tmp_ods_log_prod_expo中的记录 */
        JavaDStream<Row> odsLogProdExpoRowRdd = odsLogProdExpoEventRdd.map(new Function<DHEvent, Row>() {
            @Override
            public Row call(DHEvent event) throws Exception {
                String site = null;
                if (event.getTags().get("site") != null && !"".equals(event.getTags().get("site").toString())) {
                    site = event.getTags().get("site").toString();
                } else {
                    String siteTmp = PigConv.getSitebyUrl(event.getU());
                    if (siteTmp == null || "".equals(siteTmp)) {
                        site = "www";
                    } else {
                        site = siteTmp;
                    }
                }
                String lang = null;
                if (event.getTags().get("lang") != null && !"".equals(event.getTags().get("lang").toString())) {
                    lang = event.getTags().get("lang").toString();
                } else {
                    String langTmp = PigConv.getLangbyUrl(event.getU());
                    if (langTmp == null || "".equals(langTmp)) {
                        lang = "en";
                    } else {
                        lang = langTmp;
                    }
                }
                return RowFactory.create(event.getName(),
                        event.getId(),
                        event.getVid(),
                        event.getUsrid(),
                        event.getSid(),
                        event.getVt(),
                        event.getIp(),
                        event.getUa(),
                        event.getF(),
                        event.getD(),
                        event.getRefurl(),
                        event.getUlevel(),
                        event.getAid(),
                        event.getU(),
                        event.getCou(),
                        (event.getTags().get("uuid") == null) ? "" : event.getTags().get("uuid").toString(),
                        (event.getTags().get("pid") == null) ? "" : event.getTags().get("pid").toString(),
                        (event.getTags().get("pic") == null) ? "" : event.getTags().get("pic").toString(),
                        (event.getTags().get("dispcid") == null) ? "" : event.getTags().get("dispcid").toString(),
                        (event.getTags().get("pname") == null) ? "" : event.getTags().get("pname").toString(),
                        (event.getTags().get("ptype") == null) ? "" : event.getTags().get("ptype").toString(),
                        (event.getTags().get("pos") == null) ? "" : event.getTags().get("pos").toString(),
                        (event.getTags().get("price") == null) ? "" : event.getTags().get("price").toString(),
                        (event.getTags().get("page") == null) ? "" : event.getTags().get("page").toString(),
                        (event.getTags().get("imploc") == null) ? "" : event.getTags().get("imploc").toString(),
                        (event.getTags().get("plink") == null) ? "" : event.getTags().get("plink").toString(),
                        (event.getTags().get("supplierid") == null) ? "" : event.getTags().get("supplierid").toString(),
                        (event.getTags().get("sname") == null) ? "" : event.getTags().get("sname").toString(),
                        (event.getTags().get("cid") == null) ? "" : event.getTags().get("cid").toString(),
                        (event.getTags().get("schtype") == null) ? "" : event.getTags().get("schtype").toString(),
                        (event.getTags().get("schkw") == null) ? "" : event.getTags().get("schkw").toString(),
                        site,
                        lang,
                        (event.getTags().get("ordlang") == null) ? "" : event.getTags().get("ordlang").toString(),
                        (event.getTags().get("operation_system") == null) ? "" : event.getTags().get("operation_system").toString(),
                        //(event.getTags().get("schkw") == null) ? "" : event.getTags().get("search_keywords_md5").toString(),
                        (event.getTags().get("psum") == null) ? "" : event.getTags().get("psum").toString(),
                        (event.getTags().get("feedback") == null) ? "" : event.getTags().get("feedback").toString(),
                        (event.getTags().get("qs") == null) ? "" : event.getTags().get("qs").toString(),
                        (event.getTags().get("slink") == null) ? "" : event.getTags().get("slink").toString(),
                        (event.getTags().get("proctime") == null) ? "" : event.getTags().get("proctime").toString(),
                        (event.getTags().get("minorder") == null) ? "" : event.getTags().get("minorder").toString(),
                        (event.getTags().get("ifspring") == null) ? "" : event.getTags().get("ifspring").toString(),
                        (event.getTags().get("iffreeship") == null) ? "" : event.getTags().get("iffreeship").toString(),
                        (event.getTags().get("ifstock") == null) ? "" : event.getTags().get("ifstock").toString(),
                        (event.getTags().get("ifcou") == null) ? "" : event.getTags().get("ifcou").toString(),
                        (event.getTags().get("ifdp") == null) ? "" : event.getTags().get("ifdp").toString(),
                        (event.getTags().get("if48h") == null) ? "" : event.getTags().get("if48h").toString(),
                        (event.getTags().get("ifgold") == null) ? "" : event.getTags().get("ifgold").toString(),
                        (event.getTags().get("ifban") == null) ? "" : event.getTags().get("ifban").toString(),
                        (event.getTags().get("online") == null) ? "" : event.getTags().get("online").toString(),
                        (event.getTags().get("msge") == null) ? "" : event.getTags().get("msge").toString(),
                        (event.getTags().get("bpic") == null) ? "" : event.getTags().get("bpic").toString(),
                        (event.getTags().get("prozone") == null) ? "" : event.getTags().get("prozone").toString(),
                        (event.getTags().get("pubcid") == null) ? "" : event.getTags().get("pubcid").toString(),
                        (event.getTags().get("srs") == null) ? "" : event.getTags().get("srs").toString(),
                        (event.getTags().get("adgroup_id") == null) ? "" : event.getTags().get("adgroup_id").toString(),
                        (event.getTags().get("campaign_id") == null) ? "" : event.getTags().get("campaign_id").toString()
                );
            }
        });
        System.out.println("odsLogProdExpoRowRdd = ");
        odsLogProdExpoRowRdd.print(10);

        odsLogProdExpoRowRdd.foreachRDD(new Function2<JavaRDD<Row>, Time, Void>() {

            @Override
            public Void call(JavaRDD<Row> rdd, Time time) throws Exception {
                if (!rdd.isEmpty()) {
                    List<StructField> structFields = new ArrayList<>();
                    structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
                    structFields.add(DataTypes.createStructField("vid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("usrid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("sid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("vt", DataTypes.LongType, true));
                    structFields.add(DataTypes.createStructField("ip", DataTypes.LongType, true));
                    structFields.add(DataTypes.createStructField("ua", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("f", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("d", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("refurl", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ulevel", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("aid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("u", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("cou", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("uuid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pic", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("dispcid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pname", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ptype", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pos", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("price", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("page", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("imploc", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("plink", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("supplierid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("sname", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("cid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("schtype", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("site", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ordlang", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("operation_system", DataTypes.StringType, true));
                    //structFields.add(DataTypes.createStructField("schkw", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("psum", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("feedback", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("qs", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("slink", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("proctime", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("minorder", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ifspring", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("iffreeship", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ifstock", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("iffac", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ifcou", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ifdp", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("if48h", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ifgold", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("ifban", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("online", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("msge", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("bpic", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("prozone", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("pubcid", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("srs", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("adgroup_id", DataTypes.StringType, true));
                    structFields.add(DataTypes.createStructField("campaign_id", DataTypes.StringType, true));
                    StructType structType = DataTypes.createStructType(structFields);

                    HiveContext hiveContext = broadcastHC.value();
                    DataFrame df = hiveContext.createDataFrame(rdd, structType);
                    df.registerTempTable("tmp_ods_log_search_s");

                    hiveContext.sql("set hive.exec.dynamic.partition=true");
                    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
                    hiveContext.sql("insert into sparkstreaming.tmp_ods_log_prod_expo partition(dt) select * from tmp_ods_log_prod_expo");
                }
                return null;
            }
        });
        odsLogProdExpoKafkaRdd.foreachRDD(new Function2<JavaRDD<MessageAndMetadata<String, String>>, Time, Void>() {
            @Override
            public Void call(JavaRDD<MessageAndMetadata<String, String>> rdd, Time time) throws Exception {
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
        });
        /*
         * Click_U0001 -> tmp_ods_log_clickevent -> ods_log_clickevent
         * */
        /* get offset from zookeeper */
        String zkKafkaServers = "d1-datanode35:2181";
        String zkOffSetManager = zkKafkaServers;
        String groupID = "etl_dh_data";
        String topic = "ods_log_rec_s";
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
        Map<String,String> odsLogClickeventKafkaParameters = new HashMap<>();
        odsLogClickeventKafkaParameters.put("metadata.broker.list", "d1-kafka1:9092,d1-kafka2:9092");
        odsLogClickeventKafkaParameters.put("auto.offset.reset", "smallest");
        odsLogClickeventKafkaParameters.put("group.id", "etl_dh_data");
        Set<String> odsLogClickeventTopics =new HashSet<>();
        odsLogClickeventTopics.add("ods_log_clickevent");
        @SuppressWarnings("unchecked")
        Class<MessageAndMetadata<String, String>> streamClass =
                (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;

        JavaInputDStream<MessageAndMetadata<String, String>> odsLogClickeventKafkaRdd = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                streamClass,
                odsLogClickeventKafkaParameters,
                topicOffsets,
                Functions.<MessageAndMetadata<String, String>>identity());
        odsLogClickeventKafkaRdd.print(10);
        //odsLogClickeventKafkaRdd.persist();
        /* 将kafka中数据转换成DHEvent */
        JavaDStream<DHEvent> odsLogClickeventEventRdd = odsLogClickeventKafkaRdd.map(new Function<MessageAndMetadata<String,String>, DHEvent>() {
            @Override
            public DHEvent call(MessageAndMetadata<String,String> message) throws Exception {
                String messageSum = message.message();
                String eventBody = messageSum.substring(messageSum.indexOf("Click_U0001"));
                DHEvent event = DHEvent.parse(eventBody);

                String eventHeader = messageSum.substring(0, messageSum.indexOf("Click_U0001"));
                int startIndex = eventHeader.indexOf("timestamp") + "timestamp".length();
                String curTimeMs = eventHeader.substring(startIndex + 1, startIndex + 1 + 13); // "149821365588"
                java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String curTime = formatter.format(new Date(Long.parseLong(curTimeMs)));
                String curDate = curTime.substring(0, "yyyy-MM-dd".length());
                event.getTags().put("currentDate", curDate);
                return event;
            }
        });
         /* 将event对应转换成tmp_ods_log_clickevent中的记录 */
        JavaDStream<Row> odsLogClickeventRowRdd = odsLogClickeventEventRdd.map(new Function<DHEvent, Row>() {

            @Override
            public Row call(DHEvent event) throws Exception {
                String site = null;
                if (event.getTags().get("site") != null && !"".equals(event.getTags().get("site").toString())) {
                    site = event.getTags().get("site").toString();
                } else {
                    String siteTmp = PigConv.getSitebyUrl(event.getU());
                    if (siteTmp == null || "".equals(siteTmp)) {
                        site = "www";
                    } else {
                        site = siteTmp;
                    }
                }
                String lang = null;
                if (event.getTags().get("lang") != null && !"".equals(event.getTags().get("lang").toString())) {
                    lang = event.getTags().get("lang").toString();
                } else {
                    String langTmp = PigConv.getLangbyUrl(event.getU());
                    if (langTmp == null || "".equals(langTmp)) {
                        lang = "en";
                    } else {
                        lang = langTmp;
                    }
                }
                return RowFactory.create(event.getName(),
                        event.getId(),
                        event.getVid(),
                        event.getTags().get("usrid") != null && ! "".equals(event.getTags().get("usrid").toString()),
                        event.getSid(),
                        event.getVt(),
                        event.getIp(),
                        event.getUa(),
                        event.getF(),
                        event.getD(),
                        event.getRefurl(),
                        event.getRefpvid(),
                        event.getPvid(),
                        event.getUlevel(),
                        event.getAid(),
                        event.getU(),
                        event.getCou(),
                        site,
                        lang,
                        (event.getTags().get("pt") == null) ? "" : event.getTags().get("page_type").toString(),
                        (event.getTags().get("loc") == null) ? "" : event.getTags().get("location").toString(),
                        (event.getTags().get("attach") == null) ? "" : event.getTags().get("attach").toString(),
                        (event.getTags().get("deviceid") == null) ? "" : event.getTags().get("deviceid").toString(),
                        (event.getTags().get("activityid") == null) ? "" : event.getTags().get("activityid").toString(),
                        //(tags#'loc' is not null and tags#'loc' matches '[0-9]*{7,12}' ? tags#'loc' : tags#'itemcode') as item_code,
                        (event.getTags().get("lastvisittime") == null) ? "" : event.getTags().get("lastvt").toString(),
                        (event.getTags().get("pvn") == null) ? "" : event.getTags().get("pvn").toString(),
                        (event.getTags().get("vnum") == null) ? "" : event.getTags().get("vnum").toString(),
                        (event.getTags().get("pagedur") == null) ? "" : event.getTags().get("pagedur").toString(),
                        (event.getTags().get("session") == null) ? "" : event.getTags().get("sessionid").toString(),
                        event.getTags().get("currentDate").toString());
            }
        });
        System.out.println("odsLogClickeventRdd = ");
        odsLogClickeventRowRdd.print(10);
         odsLogClickeventRowRdd.foreachRDD(new Function2<JavaRDD<Row>, Time, Void>() {
             @Override
             public Void call(JavaRDD<Row> rdd, Time time) throws Exception {
                 if (!rdd.isEmpty()) {
                     List<StructField> structFields = new ArrayList<>();
                     structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
                     structFields.add(DataTypes.createStructField("vid", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("usrid", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("sid", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("vt", DataTypes.LongType, true));
                     structFields.add(DataTypes.createStructField("ip", DataTypes.LongType, true));
                     structFields.add(DataTypes.createStructField("ua", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("f", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("d", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("refurl", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("refpvid", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("pvid", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("ulevel", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("aid", DataTypes.LongType, true));
                     structFields.add(DataTypes.createStructField("u", DataTypes.LongType, true));
                     structFields.add(DataTypes.createStructField("cou", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("site", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("page_type", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("location", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("position", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("attach", DataTypes.LongType, true));
                     structFields.add(DataTypes.createStructField("deviceid", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("activityid", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("item_code", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("lastvt", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("pvn", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("vnum", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("pagedur", DataTypes.StringType, true));
                     structFields.add(DataTypes.createStructField("sessionid", DataTypes.StringType, true));
                     StructType structType = DataTypes.createStructType(structFields);

                     HiveContext hiveContext = broadcastHC.value();
                     DataFrame df = hiveContext.createDataFrame(rdd, structType);
                     df.registerTempTable("tmp_ods_log_clickevent");

                     hiveContext.sql("set hive.exec.dynamic.partition=true");
                     hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
                     hiveContext.sql("INSERT INTO sparkstreaming.tmp_ods_log_clickevent PARTITION (dt) select * from tmp_ods_log_clickevent");
                 }
                 return null;
             }
         });
        odsLogClickeventKafkaRdd.foreachRDD(new Function2<JavaRDD<MessageAndMetadata<String, String>>, Time, Void>() {

            @Override
            public Void call(JavaRDD<MessageAndMetadata<String, String>> rdd, Time time) throws Exception {
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
        });
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }


}
