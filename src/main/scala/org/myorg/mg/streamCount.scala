package org.myorg.mg

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.AbstractRichFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueState
import scala.collection.mutable.HashSet
import scala.collection.Set
import org.apache.flink.streaming.api.datastream.AllWindowedStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.runtime.minicluster.FlinkMiniCluster

object streamCount {
  def main(args: Array[String]) {

    // Checking input parameters
//    val params = ParameterTool.fromArgs(args)

    if (args.length != 2) {
      System.err.println("USAGE:\nstreamCount <inputFile> <outputPath>")
      return
    }

    val inputFilePath = args(0)
    val outputFilePath= args(1)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
//    env.getConfig.setGlobalJobParameters(params)

    // get input data
//    val text =env.readTextFile(inputFilePath)
    val text = env.socketTextStream("localhost", 9999)

    val k = 10000
    val threshold = 0
    val total_decrement = 0
//
    val counts: DataStream[(String, Int)] = text

      // split up the lines in pairs (2-tuples) containing: (word,1)
        .flatMap( _.toLowerCase.split("\\W+"))
//      .flatMap(new CountWindowAverage())
      .filter(_.nonEmpty)
      .map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .sum(1)
      .keyBy(0)
      .mapWithState((in: (String, Int), count: Option[Int]) =>
        count match {
          case Some(c) => ( (in._1, c), Some(c + in._2-threshold) )
          case None => ( (in._1, 0), Some(in._2-threshold) )
        })


    // emit result
//    counts.writeAsText(outputFilePath)
    counts.print()


    // execute program
    env.execute("Streaming WordCount")
  }
}
