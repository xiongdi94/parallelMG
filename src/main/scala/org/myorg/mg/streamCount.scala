package org.myorg.mg

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.AbstractRichFunction
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.state._

import scala.collection.mutable.HashSet
import scala.collection.Set
import org.apache.flink.streaming.api.datastream.AllWindowedStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.runtime.minicluster.FlinkMiniCluster
import java.net.{InetAddress, ServerSocket, Socket, SocketException}

import org.apache.flink.api.common.state
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.util.Collector

import scala.collection.immutable.StringOps
import scala.util.hashing.Hashing
import org.apache.flink.api.scala.typeutils;

class CountWindowAverage extends RichFlatMapFunction[(String, Int, Int), (String, Int)] {

  private var sum:MapState[String, Int]=null

  override def flatMap(input: (String, Int, Int ), out: Collector[(String, Int)]): Unit = {
    // access the state value
//    val tmpCurrentSum = sum(input._1)

    // If it hasn't been used before, it will be null

    val currentSum = if (sum.contains(input._1)) {
      sum.get(input._1)

    } else {
      0
    }

    // update the count
    val newSum = currentSum + input._2

    // update the state
    sum.remove(input._1)
    sum.put(input._1,newSum)
//    it.forEach((k,v)=>print(k))


    val k=1000
    var maxV=0-1
    var maxK=""
    var count=1001
    while(count>k){
      count=0
      var i=sum.iterator()
      while(i.hasNext){
        i.next()
        count+=1
      }
      i=sum.iterator()
      if(count>k){
        while(i.hasNext){
          val e=i.next
          if(e.getValue>1){
            e.setValue(e.getValue-1)
          }
          else{
            i.remove()
//            print(e.getKey)
          }
        }
      }

    }

    val i=sum.iterator()
    while(i.hasNext){
      val e=i.next()
      if(e.getValue>maxV){
        maxV = e.getValue
        maxK = e.getKey
      }
    }


//    print(count)

    out.collect((maxK,maxV))


  }

  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getMapState[String,Int](
      new MapStateDescriptor[String,Int]("count",createTypeInformation[String],createTypeInformation[Int]))

  }
}


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


    def  g(k:String,v:Int)=Tuple3(k,v,Math.abs(Hashing.default.hash(k)%4))

    val counts: DataStream[(String, Int)] = text

      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap( _.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)

      .map{(_, 1)}
      .map{x=>g(x._1,x._2)}
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .sum(1)

      .keyBy(_._3)
//      .mapWithState((in: (String, Int), count: Option[Int]) =>
//        count match {
//          case Some(c) => ( (in._1, c), Some(c + in._2-threshold) )
//          case None => ( (in._1, 0), Some(in._2-threshold) )
//        })
      .flatMap(new CountWindowAverage())
         



      counts.print()
//    counts.writeAsText(outputFilePath)

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
//    print(Hashing.default.hash("sad"))


    // execute program
    env.execute("Streaming WordCount")
  }
}
