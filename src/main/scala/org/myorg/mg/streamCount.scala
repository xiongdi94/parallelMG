package org.myorg.mg

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.AbstractRichFunction
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.state.{MapState, ValueState, ValueStateDescriptor}

import scala.collection.mutable.HashSet
import scala.collection.Set
import org.apache.flink.streaming.api.datastream.AllWindowedStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.runtime.minicluster.FlinkMiniCluster
import java.net.{InetAddress, ServerSocket, Socket, SocketException}

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.util.Collector

//class mapWithCounter extends RichMapFunction[(String, Long)] {

//  private var sum: ValueState[Long] =_
//
//  override def map(input: (String, Long)): Uint= {
////  override def map
//
//    // access the state value
//    val tmpCurrentSum = sum.value()
//    // If it hasn't been used before, it will be null
//    val currentSum = if (tmpCurrentSum != null) {
//      tmpCurrentSum
//    } else {
//      0
//    }
//
//    // update the count
//    val newSum = currentSum+ input._2
//
//    // update the state
//    sum.update(newSum)
////    return ( (input._1, input._2), Some(newSum) )
//
////     if the count reaches 2, emit the average and clear the state
////    if (newSum >= 2) {
////      return Collector.
//
////      sum.clear()
//    }
//  }
//
//  override def open(parameters: Configuration): Unit = {
//    sum = getRuntimeContext.getState(
//      new ValueStateDescriptor[Long]("average", createTypeInformation[Long])
//    )
//  }
//}

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
//    val ia = InetAddress.getByName("localhost")
//    val socket = new Socket(ia, 9999)
    val text = env.socketTextStream("localhost", 9999)

    val k = 10000
    val threshold = 0

    val counts: DataStream[(String, Int)] = text

      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap( _.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(1))
      .sum(1)
      .keyBy(0)
      .mapWithState((in: (String, Int), count: Option[Int]) =>
        count match {
          case Some(c) => ( (in._1, c), Some(c + in._2-threshold) )
          case None => ( (in._1, 0), Some(in._2-threshold) )
        })
//      .map(new mapWithCounter())
         



//      .maxBy(1)
//        .max(10)

    counts.print()
//    counts.broadcast
//    val tableEnv = TableEnvironment.getTableEnvironment(env)
//    val table1: Table = tableEnv.fromDataStream(counts)
//    tableEnv.registerTable("table1", table1)
//    table1.printSchema()
//    //    tableEnv.registerDataStream("table1", counts)
//    val revenue = tableEnv.sqlQuery("""
//                                        |SELECT *
//                                        |FROM table1
//                                        |WHERE _2>10000
//                                      """.stripMargin)
//    // create a TableSink
//    val sink = new CsvTableSink("/home/dayouzi/IdeaProjects/parallelMG/output", fieldDelim = " ")
//
//    // METHOD 1:
//    //   Emit the result Table to the TableSink via the writeToSink() method
//    revenue.writeToSink(sink)

//    val dsRow: DataStream[String] = tableEnv.toAppendStream[String](table2)
//    dsRow.print()
    // emit result
//    counts.writeAsText(outputFilePath)




    // execute program
    env.execute("Streaming WordCount")
  }
}
