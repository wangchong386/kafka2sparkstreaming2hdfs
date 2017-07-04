package dh.bigdata.etl;

import com.dhgate.event.DHEvent;
import dh.bigdata.etl.sparkstreaming.*;
import dh.bigdata.etl.util.KafkaManager;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.util.*;

public class Main {

    public static volatile Broadcast<HiveContext> broadcastHC = null;

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("etl_dh_data").setMaster("yarn-client");
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));
        //jssc.checkpoint("");

        HiveContext hiveContext = new HiveContext(jssc.sparkContext().sc());
        hiveContext.setConf("hive.execution.engine","spark");
        broadcastHC = jssc.sc().broadcast(hiveContext);

        /**
         * Search_S0005 -> tmp_ods_log_rec_s -> ods_log_rec_s
         * 1、获取topic ods_log_rec_s kafka的streaming
         * 2、蒋kafka中数据转换成DHEvent
         * 3、将event对应转换成tmp_ods_log_rec_s中的记录
         * 4、将偏移提交到zookeeper
         */
        String topic = "ods_log_rec_s";
        JavaInputDStream<MessageAndMetadata<String, byte[]>> odsLogRecsKafkaRdd = KafkaDirecStreamFactory.get(jssc, topic);
        JavaDStream<DHEvent> odsLogRecsEventRdd = odsLogRecsKafkaRdd.map(new Message2DHEvent());
        JavaDStream<Row> odsLogRecsRowRdd = odsLogRecsEventRdd.map(new Event2OdsLogRecs());
        odsLogRecsRowRdd.foreachRDD(new InsertOdsLogRecs(broadcastHC));
        odsLogRecsKafkaRdd.foreachRDD(new UpdateOffset());

        /**
         * Public_S0003|Search_U0001|Item_U0001|Checkout_U0001|Item_U0003 -> tmp_ods_log_pageview -> ods_log_pageview
         * 1、获取topic ods_log_pageview kafka的streaming
         * 2、蒋kafka中数据转换成DHEvent
         * 3、将event对应转换成tmp_ods_log_pageview中的记录
         * 4、将偏移提交到zookeeper
         */
        topic = "ods_log_pageview";
        JavaInputDStream<MessageAndMetadata<String, byte[]>> odsLogPageViewKafkaRdd = KafkaDirecStreamFactory.get(jssc, topic);
        JavaDStream<DHEvent> odsLogPageViewEventRdd = odsLogPageViewKafkaRdd.map(new Message2DHEvent());
        JavaDStream<Row> odsLogPageViewRowRdd = odsLogPageViewEventRdd.map(new Event2OdsLogPageView());
        odsLogPageViewRowRdd.foreachRDD(new InsertOdsLogPageView(broadcastHC));
        odsLogPageViewKafkaRdd.foreachRDD(new UpdateOffset());

        /**
         * Search_S0001 -> tmp_ods_log_search_s -> ods_log_search_s
         * 1、获取topic ods_log_search_s kafka的streaming
         * 2、蒋kafka中数据转换成DHEvent
         * 3、将event对应转换成tmp_ods_log_search_s中的记录
         * 4、将偏移提交到zookeeper
         */
        topic = "ods_log_search_s";
        JavaInputDStream<MessageAndMetadata<String, byte[]>> odsLogSearchsKafkaRdd = KafkaDirecStreamFactory.get(jssc, topic);
        JavaDStream<DHEvent> odsLogSearchsEventRdd = odsLogSearchsKafkaRdd.map(new Message2DHEvent());
        JavaDStream<Row> odsLogSearchsRowRdd = odsLogSearchsEventRdd.map(new Event2OdsLogSearchs());
        odsLogSearchsRowRdd.foreachRDD(new InsertOdsLogSearchs(broadcastHC));
        odsLogSearchsKafkaRdd.foreachRDD(new UpdateOffset());

        /**
         * Public_S0001 -> tmp_ods_log_prod_expo -> ods_log_prod_expo
         * 1、获取topic ods_log_prod_expo kafka的streaming
         * 2、蒋kafka中数据转换成DHEvent
         * 3、将event对应转换成tmp_ods_log_prod_expo中的记录
         * 4、将偏移提交到zookeeper
         */
        topic = "ods_log_prod_expo";
        JavaInputDStream<MessageAndMetadata<String, byte[]>> odsLogProdExpoKafkaRdd = KafkaDirecStreamFactory.get(jssc, topic);
        JavaDStream<DHEvent> odsLogProdExpoEventRdd = odsLogProdExpoKafkaRdd.map(new Message2DHEvent());
        JavaDStream<Row> odsLogProdExpoRowRdd = odsLogProdExpoEventRdd.map(new Event2OdsLogProdExpo());
        odsLogProdExpoRowRdd.foreachRDD(new InsertOdsLogProdExpo(broadcastHC));
        odsLogProdExpoKafkaRdd.foreachRDD(new UpdateOffset());


        /**
         * Click_U0001 -> tmp_ods_log_clickevent -> ods_log_clickevent
         * 1、获取topic ods_log_clickevent kafka的streaming
         * 2、蒋kafka中数据转换成DHEvent
         * 3、将event对应转换成tmp_ods_log_clickevent中的记录
         * 4、将偏移提交到zookeeper
         */
        topic = "ods_log_clickevent";
        JavaInputDStream<MessageAndMetadata<String, byte[]>> odsLogClickEventKafkaRdd = KafkaDirecStreamFactory.get(jssc, topic);
        JavaDStream<DHEvent> odsLogClickEventRdd = odsLogClickEventKafkaRdd.map(new Message2DHEvent());
        JavaDStream<Row> odsLogClickEventRowRdd = odsLogClickEventRdd.map(new Event2OdsLogClickEvent());
        odsLogClickEventRowRdd.foreachRDD(new InsertOdsLogClickEvent(broadcastHC));
        odsLogClickEventKafkaRdd.foreachRDD(new UpdateOffset());

        /* Search_S0002 -> tmp_ods_log_subject_expo -> ods_log_subject_expo
        * 1. 获取topic ods_log_subject_expo kafka的streaming
        * 2. 将kafka中的数据转换为DHEvent
        * 3. 将event对应转换成tmp_ods_log_subject_expo中的记录
        * 4. 将便宜提交到zookeeper
        */

        topic = "ods_log_subject_expo";
        JavaInputDStream<MessageAndMetadata<String, byte[]>> odsLogSubjectExpoKafkaRdd = KafkaDirecStreamFactory.get(jssc,topic);
        JavaDStream<DHEvent> odsLogSubjectExpoEventRdd = odsLogSubjectExpoKafkaRdd.map(new Message2DHEvent());
        JavaDStream<Row> odsLogSubjectExpoRowRdd = odsLogSubjectExpoEventRdd.map(new Event2OdsLogSubjectExpo());
        odsLogSubjectExpoRowRdd.foreachRDD(new InsertOdsLogSubjectExpo(broadcastHC));
        odsLogSubjectExpoKafkaRdd.foreachRDD(new UpdateOffset());

       /* App_U0001 ->  tmp_ods_log_app -> ods_log_app
        * 1. 获取topic ods_log_app kafka的streaming
        * 2. 将kafka中的数据转换为DHEvent
        * 3. 将event对应转换成tmp_ods_log_app中的记录
        * 4. 将便宜提交到zookeeper
        */
        topic = "ods_log_app";
        JavaInputDStream<MessageAndMetadata<String, byte[]>> odsLogAppKafkaRdd = KafkaDirecStreamFactory.get(jssc,topic);
        JavaDStream<DHEvent> odsLogAppEventRdd = odsLogAppKafkaRdd.map(new Message2DHEvent());
        JavaDStream<Row> odsLogAppRowRdd = odsLogAppEventRdd.map(new Event2OdsLogApp());
        odsLogAppRowRdd.foreachRDD(new InsertOdsLogApp(broadcastHC));
        odsLogAppKafkaRdd.foreachRDD(new UpdateOffset());


         /* APP_D0001 ->  tmp_ods_log_app_device -> ods_log_app_device
        * 1. 获取topic ods_log_app_device kafka的streaming
        * 2. 将kafka中的数据转换为DHEvent
        * 3. 将event对应转换成tmp_ods_log_app_device中的记录
        * 4. 将便宜提交到zookeeper
        */
        topic = "ods_log_app_device";
        JavaInputDStream<MessageAndMetadata<String, byte[]>> odsLogAppDeviceKafkaRdd = KafkaDirecStreamFactory.get(jssc,topic);
        JavaDStream<DHEvent> odsLogAppDeviceEventRdd = odsLogAppDeviceKafkaRdd.map(new Message2DHEvent());
        JavaDStream<Row> odsLogAppDeviceRowRdd = odsLogAppDeviceEventRdd.map(new Event2OdsLogAppDevice());
        odsLogAppDeviceRowRdd.foreachRDD(new InsertOdsLogAppDevice(broadcastHC));
        odsLogAppDeviceKafkaRdd.foreachRDD(new UpdateOffset());

         /* APP_E0001 ->  tmp_ods_log_app_events -> ods_log_app_events
        * 1. 获取topic ods_log_app_events kafka的streaming
        * 2. 将kafka中的数据转换为DHEvent
        * 3. 将event对应转换成tmp_ods_log_app_events中的记录
        * 4. 将便宜提交到zookeeper
        */
        topic = "ods_log_app_events";
        JavaInputDStream<MessageAndMetadata<String, byte[]>> odsLogAppEventsKafkaRdd = KafkaDirecStreamFactory.get(jssc,topic);
        JavaDStream<DHEvent> odsLogAppEventsEventRdd = odsLogAppEventsKafkaRdd.map(new Message2DHEvent());
        JavaDStream<Row> odsLogAppEventsRowRdd = odsLogAppEventsEventRdd.map(new Event2OdsLogAppEvents());
        odsLogAppEventsRowRdd.foreachRDD(new InsertOdsLogAppEvents(broadcastHC));
        odsLogAppEventsKafkaRdd.foreachRDD(new UpdateOffset());

        /* APP_E0002 ->  tmp_ods_log_app_events_ios -> ods_log_app_events_ios
        * 1. 获取topic ods_log_app_events_ios kafka的streaming
        * 2. 将kafka中的数据转换为DHEvent
        * 3. 将event对应转换成tmp_ods_log_app_events_ios中的记录
        * 4. 将便宜提交到zookeeper
        */

        topic = "ods_log_app_events_ios";
        JavaInputDStream<MessageAndMetadata<String, byte[]>> odsLogAppEventsIosKafkaRdd = KafkaDirecStreamFactory.get(jssc,topic);
        JavaDStream<DHEvent> odsLogAppEventsIosEventRdd = odsLogAppEventsIosKafkaRdd.map(new Message2DHEvent());
        JavaDStream<Row> odsLogAppEventsIosRowRdd = odsLogAppEventsIosEventRdd.map(new Event2OdsLogAppEventsIos());
        odsLogAppEventsIosRowRdd.foreachRDD(new InsertOdsLogAppEventsIos(broadcastHC));
        odsLogAppEventsIosKafkaRdd.foreachRDD(new UpdateOffset());


        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }


}
