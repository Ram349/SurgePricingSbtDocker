import kafka.utils.ZkUtils
import java.util.Properties
import kafka.admin.AdminUtils


object KafkaTopicCreator {
      def main(args : Array[String]) = {
    
      val sessionTimeoutMs = 10000;
      val connectionTimeoutMs = 10000;
      val zookeeperClient = kafka.utils.ZkUtils.createZkClient("localhost:2181", sessionTimeoutMs, connectionTimeoutMs);
      
      val numPartitions = 4;
      val replicationFactor = 1;
      val topicConfig = new Properties();
      val topicsSet = Set("dr5rvz"
                        ,"dr5rtb"
                        ,"dr5r7v"
                        ,"dr72w8"
                        ,"dr5xbg"
                        ,"dr7810"
                        ,"dr5rq4"
                        ,"dr5re9"
                        ,"dr5x86"
                        ,"dr5rp0"
                        ,"dr5rnn"
                        ,"dr5rsh"
                        ,"dr5rmq"
                        ,"dr72qs"
                        ,"dr5q8p"
                        ,"dr72rh"
                        ,"dr5ruu"
                        ,"dr72q6"
                        ,"dr5rkq"
                        ,"dr5qup"
                        ,"dr5qv5"
                        ,"dr5x2u"
                        ,"dr5rh1"
                        ,"dr5rvu"
                        ,"dr5r01"
                        ,"dr5qtz"
                        ,"dr5rsc"
                        ,"dr5r57"
                        ,"dr5nw9"
                        ,"dr5ruw"
                        ,"dr5xc7"
                        ,"dr782w"
                        ,"dr72j7"
                        ,"dr72xc"
                        ,"dr5ruh"
                        ,"dr5rkz"
                        ,"dr72rq"
                        ,"dr72pb"
                        ,"dr72rn"
                        ,"dr5qfz"
                        ,"dr5x6p"
                        ,"dr5quf"
                        ,"dr5r2p"
                        ,"dr5rkj"
                        ,"dr72hg"
                        ,"dr5rkn"
                        ,"dr5x01"
                        ,"dr5ree"
                        ,"dr5rst"
                        ,"dr5rs0"
                        ,"dr5rhk"
                        ,"dr5xdr"); 
      
      for(topic <- topicsSet){
                  //AdminUtils.createTopic(zookeeperClient, topic, numPartitions, replicationFactor, topicConfig) 
      }
  }
}