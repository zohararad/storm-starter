package com.zohararad;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class Topology {

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    // create a new instance of spout (stream entry point)
    // this dumps a new random sentence into stream every 1s
    RandomSentenceSpout inputSpout = new RandomSentenceSpout();

    // set the spout as entry point to default stream
    builder.setSpout("stream-input", inputSpout);

    // set split sentence bolt as handler of random sentence spout.
    // in other words this is the first bolt in the topology - it's positioned right after the spout
    builder.setBolt("split-sentence", new SplitSentenceBolt(), 3)
        .shuffleGrouping("stream-input"); //shuffleGrouping places the bolt after the spout identified by "stream-input"

    // set word count bolt after split sentence bolt
    builder.setBolt("count-words", new WordCountBolt(), 3)
        .fieldsGrouping("split-sentence", new Fields("word"));

    Config conf = new Config();
    conf.setDebug(false);
    conf.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", conf, builder.createTopology());
  }
}
