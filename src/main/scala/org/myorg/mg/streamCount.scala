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
import org.apache.flink.api.scala.typeutils

import scala.util.Random
import java.util.Calendar

import org.apache.flink.core.fs





object streamCount {
  var numOfMaps=128 //num of maps kept in state
  class CountWindow extends RichFlatMapFunction[(String, Int, Int), (String,List[(String, Int, Int)])] {

    //  sum(Key=>(count,total_decrement))
    private var sum:MapState[String, (Int, Int)]=null
    private var timeInMillis=System.currentTimeMillis()

    override def flatMap(input: (String, Int, Int ), out: Collector[(String,List[(String, Int, Int)])]): Unit = {

      //Update map using new input
      if (sum.contains(input._1)) {
        val tmp_sum=sum.get(input._1)
        sum.remove(input._1)
        sum.put(input._1,(tmp_sum._1+input._2,tmp_sum._2))

      }
      else {
        sum.put(input._1,(input._2,0))
      }


      val k=10000/numOfMaps                  //number of items kept in map
      val frequentThreshold=0      //judge whether a word is frequent
      val time_interval=3000       //time interval for every collection (milliseconds)
      var count=Int.MaxValue

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
            if(e.getValue._1>1){
              e.setValue(e.getValue._1-1,e.getValue._2+1)
            }
            else{
              i.remove()
            }
          }
        }

      }

      if( System.currentTimeMillis()-timeInMillis>time_interval) {
        timeInMillis = System.currentTimeMillis()
        var storedElems: List[(String, Int, Int)] = Nil
        val i = sum.iterator()
        while (i.hasNext) {
          val e = i.next()
          //only put enough frequent words into list
          if (e.getValue._1 > frequentThreshold) {
            storedElems = storedElems.::(e.getKey, e.getValue._1, e.getValue._2)
          }

        }

        val storedElems_sorted = storedElems.sortBy(_._2)(Ordering[Int].reverse)


        //collect top 20 frequent words
        if (!storedElems_sorted.isEmpty  ) {
          //        print(input._3)
          val now=Calendar.getInstance()
          val currentTime=now.get(Calendar.YEAR).toString+"/"+
            now.get(Calendar.MONTH).toString+"/"+
            now.get(Calendar.DATE)+" "+
            now.get(Calendar.HOUR_OF_DAY)+":"+
            now.get(Calendar.MINUTE)+":"+
            now.get(Calendar.SECOND)
          out.collect((currentTime, storedElems_sorted.take(20)))

        }
      }



    }

    override def open(parameters: Configuration): Unit = {
      sum = getRuntimeContext.getMapState[String,(Int, Int)](
        new MapStateDescriptor[String,(Int, Int)]("count",createTypeInformation[String],createTypeInformation[(Int,Int)]))

    }
  }

  def main(args: Array[String]) {
    var inputFilePath=""
    var outputFilePath=""
    var numOfParallelism=4
    // Checking input parameters
    if (args.length == 3) {
//      System.err.println("USAGE:\nstreamCount <inputFile> <outputPath>")
//      return
       inputFilePath = args(0)
       outputFilePath= args(1)
       numOfParallelism=args(2).toInt
//       numOfMaps=args(3).toInt
    }
    else{
      inputFilePath="/home/xiong/IdeaProjects/parallelMG/input/wiki.train.tokens"
//      inputFilePath="/home/xiong/wiki.train.tokens"
      outputFilePath="/home/xiong/IdeaProjects/parallelMG/output"
    }





    //    val numOfParallelism= Runtime.getRuntime.availableProcessors

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(numOfParallelism)
    // make parameters available in the web interface
//    env.getConfig.setGlobalJobParameters(params)

    val Bash_proc=new ProcessBuilder("bash","./clear.sh")
    Bash_proc.start()

//    val Python_proc = new ProcessBuilder("python","./server.py")
//    Python_proc.start()s

    // get input data
    val text =env.readTextFile(inputFilePath)
//    val text = env.socketTextStream("localhost", 6666)

//    val rd=new Random(123)
    def  g(k:String,v:Int)=Tuple3(k,v,Math.abs(Hashing.default.hash(k))%numOfMaps)
//    def  g(k:String,v:Int)=Tuple3(k,v,v%4+1)


    val counts = text
      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap( _.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)

      .map{(_, 1)}
      .keyBy(0)
      .timeWindow(Time.seconds(1))
//      .map{x=>g(x._1,x._2)}
//      .map{x=>g(x._1,x._2)}
//        .timeWindowAll(Time.seconds(3))
//      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
      .sum(1)
//      .reduce{(v1, v2) => (v1._1, v1._2 + v2._2)}
      .map{x=>g(x._1,x._2)}


      .keyBy(_._3)
      //      .keyBy(0)
//      .sum(1)


//////      .mapWithState((in: (String, Int), count: Option[Int]) =>
//////        count match {
//////          case Some(c) => ( (in._1, c), Some(c + in._2-threshold) )
//////          case None => ( (in._1, 0), Some(in._2-threshold) )
//////        })
      .flatMap(new CountWindow())

         



//      counts.print()
//      counts.writeAsText(outputFilePath,fs.FileSystem.WriteMode.OVERWRITE)

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
