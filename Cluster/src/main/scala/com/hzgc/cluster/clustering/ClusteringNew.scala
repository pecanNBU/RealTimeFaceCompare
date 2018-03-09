package com.hzgc.cluster.clustering

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Properties}

import breeze.linalg.Transpose
import com.hzgc.cluster.consumer.PutDataToEs
import com.hzgc.cluster.util.PropertiesUtils
import com.hzgc.dubbo.clustering.ClusteringAttribute
import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object ClusteringNew {

  case class Data(id: Long, time: Timestamp, ipc: String, host: String, spic: String, bpic: String)

  case class Clustering(firstUrl: String, dataSet: mutable.Set[String])

  val LOG: Logger = Logger.getLogger(KMeansClustering.getClass)
  val threshold: Double = 0.9
  val timeCount: Int = 30
  val repetitionRate: Double = 0.4


  def main(args: Array[String]) {

    val driverClass = "com.mysql.jdbc.Driver"
    val sqlProper = new Properties()
    val properties = PropertiesUtils.getProperties
    val clusterNum = properties.getProperty("job.clustering.cluster.number")
    val iteraterNum = properties.getProperty("job.clustering.iterater.number")
    val appName = properties.getProperty("job.clustering.appName")
    val url = properties.getProperty("job.clustering.mysql.url")
    val tableName = properties.getProperty("job.clustering.mysql.table")
    val timeField = properties.getProperty("job.clustering.mysql.field.time")
    val ipcField = properties.getProperty("job.clustering.mysql.field.ipc")
    val dataField = properties.getProperty("job.clustering.mysql.field.data")
    val idField = properties.getProperty("job.clustering.mysql.field.id")
    val hostField = properties.getProperty("job.clustering.mysql.field.host")
    val spicField = properties.getProperty("job.clustering.mysql.field.spic")
    val bpicField = properties.getProperty("job.clustering.mysql.field.bpic")
    val partitionNum = properties.getProperty("job.clustering.partiton.number").toInt

    //val spark = SparkSession.builder().appName(appName).enableHiveSupport().master("local[*]").getOrCreate()
    val spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
    import spark.implicits._

    val calendar = Calendar.getInstance()
    val currentYearMon = "'" + calendar.get(Calendar.YEAR) + "-%" + (calendar.get(Calendar.MONTH)) + "%'"

    spark.sql("select ftpurl,feature from person_table where date like " + currentYearMon).createOrReplaceTempView("parquetTable")

    val preSql = "(select T1.id, T2.host_name, T2.big_picture_url, T2.small_picture_url, T1.alarm_time " + "from t_alarm_record as T1 inner join t_alarm_record_extra as T2 on T1.id=T2.record_id " + "where T2.static_id IS NULL " + "and DATE_FORMAT(T1.alarm_time,'%Y-%m') like " + currentYearMon + ") as temp"

    sqlProper.setProperty("driver", driverClass)
    val dataSource = spark.read.jdbc(url, preSql, sqlProper)
    dataSource.map(data => {
      println("ftp://" + data.getAs[String](hostField) + ":2121" + data.getAs[String](spicField))
      Data(data.getAs[Long](idField), data.getAs[Timestamp](timeField), data.getAs[String](spicField).substring(1, data.getAs[String](spicField).indexOf("/", 1)), data.getAs[String](hostField), "ftp://" + data.getAs[String](hostField) + ":2121" + data.getAs[String](spicField), "ftp://" + data.getAs[String](hostField) + ":2121" + data.getAs[String](bpicField))
    }).createOrReplaceTempView("mysqlTable")

    val joinData = spark.sql("select T1.feature, T2.* from parquetTable as T1 inner join mysqlTable as T2 on T1.ftpurl=T2.spic").repartition(40)
    //get the url and feature
    val idPointDS = joinData.map(data => (data.getAs[String]("spic"),data.getAs[mutable.WrappedArray[Float]]("feature").toArray
      .map(_.toDouble)))
    // TODO:
    val idPointDS2 = joinData.map(data => (data.getAs[String]("spic"),Vectors.dense(
      data.getAs[mutable.WrappedArray[Float]]("feature").toArray
      .map(_.toDouble))))
    //zipwithIndex for decrease the computer cost
    val zipIdPointDs = idPointDS.rdd.zipWithIndex()
    val joined = zipIdPointDs.cartesian(zipIdPointDs)
    val dataPairs = joined.filter(f => f._1._2 < f._2._2)

    //calculate the cosine similarity of each two data
    val pairSimilarity = dataPairs.map(data => {
      (data._1._1._1, data._2._1._1, cosineMeasure(data._1._1._2, data._2._1._2))
    })
    //filter by the similarity
    val filterSimilarity = pairSimilarity.filter(_._3 > threshold)

    //count each clutering data number,the first image crashed
    val furlGroup = filterSimilarity.map(data => (data._1, data._2)).reduceByKey((a, b) => a + "," + b)
    val numPerUrl = furlGroup.map(data => {
      val valList = data._2.split(",").toList
      println(valList)
      (data._1, valList, valList.size)
    }).filter(_._3 > timeCount)

    //calculate the similarity of two
    val numPerUrlZip = numPerUrl.zipWithIndex()
    val joinNumFliter = numPerUrlZip.cartesian(numPerUrlZip).filter(f => f._1._2 < f._2._2)
    val unionData = joinNumFliter.map(data =>
      (data._1._1._1, data._2._1._1, data._1._1._2, data._2._1._2, dataSetSimilarity(data._1._1._2, data._2._1._2)))
      .filter(data => data._5 > repetitionRate)

    val lastData = unionData.map(data => {
      val unionList = data._3.union(data._4).distinct
      (data._1, unionList)
    }).groupBy(key=>key._1).foreach(println(_))

    /*val mon = calendar.get(Calendar.MONTH)
    //+1
    var monStr = ""
    if (mon < 10) {
      monStr = "0" + mon
    } else {
      monStr = String.valueOf(mon)
    }
    val yearMon = calendar.get(Calendar.YEAR) + "-" + monStr
    val clusteringRowKey = yearMon + "region"
    var i = 0
    val viewData = joinData.select("id", "time", "ipc", "host", "spic", "bpic")
    val table1List = new util.ArrayList[ClusteringAttribute]()
    numPerUrl.map(data => {
      val clusteringAttribute = new ClusteringAttribute
      clusteringAttribute.setClusteringId(i.toString)
      clusteringAttribute.setCount(data._3)
      clusteringAttribute.setFtpUrl(data._1)
      val dataListDF = spark.sparkContext.makeRDD(data._2).toDF()
      dataListDF.printSchema()
      val fullInfoDf = dataListDF.join(viewData, dataListDF("_1") === viewData("spic"))
      val orderData = fullInfoDf.orderBy(fullInfoDf("time"))
      val idList = new util.ArrayList[Integer]()
      val rowKey = clusteringRowKey + i
      val putDataToEs = PutDataToEs.getInstance()
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      orderData.foreach(
        data => {
          val date = new Date(data.getAs[Timestamp]("time").getTime)
          val dateNew = sdf.format(date)
          val status = putDataToEs.upDateDataToEs(data.getAs[String]("spic"), rowKey, dateNew, data.getAs[Long]("id").toInt)
          if (status != 200) {
            LOG.info("Put data to es failed! And the failed ftpurl is " + data.getAs("spic"))
          }
        }
      )
      val first = orderData.first()
      val last = orderData.orderBy(-orderData("time")).first()
      clusteringAttribute.setFirstAppearTime(first.getAs("time").toString)
      clusteringAttribute.setFirstIpcId(first.getAs("ipc"))
      clusteringAttribute.setLastAppearTime(last.getAs("time").toString)
      clusteringAttribute.setLastIpcId(last.getAs("ipc"))
      i += 1
      clusteringAttribute
    }).foreach(data => table1List.add(data))
    PutDataToHBase.putClusteringInfo(clusteringRowKey, table1List)*/
    spark.stop()
  }

  def cosineMeasure(v1: Array[Double], v2: Array[Double]): Double = {

    val member = v1.zip(v2).map(d => d._1 * d._2).reduce(_ + _).toDouble
    //求出分母第一个变量值
    val temp1 = math.sqrt(v1.map(num => {
      math.pow(num, 2)
    }).reduce(_ + _))
    //求出分母第二个变量值
    val temp2 = math.sqrt(v2.map(num => {
      math.pow(num, 2)
    }).reduce(_ + _))
    //求出分母
    val denominator = temp1 * temp2
    //进行计算
    member / denominator
  }

  def dataSetSimilarity(list1: List[String], list2: List[String]): Double = {
    //union size of two list
    val union = List.concat(list1, list2).distinct.size
    //intersect size of two list
    val intersect = list1.intersect(list2).size
    val minSize = if (list1.size < list2.size) list1.size else list2.size
    intersect / minSize
  }
}
