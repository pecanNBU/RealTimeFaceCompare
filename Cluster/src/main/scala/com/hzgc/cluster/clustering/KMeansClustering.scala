package com.hzgc.cluster.clustering

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Properties, UUID}

import com.hzgc.cluster.clustering.KMeansClustering2.LOG
import com.hzgc.cluster.consumer.PutDataToEs
import com.hzgc.cluster.util.PropertiesUtils
import com.hzgc.dubbo.clustering.ClusteringAttribute
import org.apache.log4j.Logger
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._

import scala.collection.mutable

object KMeansClustering {

  case class Data(id: Long, time: Timestamp, ipc: String, host: String, spic: String, bpic: String)

  case class CenterData(num: Int, data: Array[Double])

  val LOG: Logger = Logger.getLogger(KMeansClustering.getClass)
  val numClusters = 50
  val numIterations = 100000
  var clusterIndex: Int = 0
  val similarityThreshold = 0.90
  val appearCount = 10

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

    val spark = SparkSession.builder().appName(appName).enableHiveSupport().master("local[*]").getOrCreate()
    import spark.implicits._

    val calendar = Calendar.getInstance()
    val currentYearMon = "'" + calendar.get(Calendar.YEAR) + "-%" + (calendar.get(Calendar.MONTH)) + "%'"

    spark.sql("select ftpurl,feature from person_table where date like " + currentYearMon).createOrReplaceTempView("parquetTable")

    val preSql = "(select T1.id, T2.host_name, T2.big_picture_url, T2.small_picture_url, T1.alarm_time " +
      "from t_alarm_record as T1 inner join t_alarm_record_extra as T2 on T1.id=T2.record_id " +
      "where T2.static_id IS NULL " + "and DATE_FORMAT(T1.alarm_time,'%Y-%m') like " + currentYearMon + ") as temp"

    val dataSource = spark.read.jdbc(url, preSql, sqlProper)
    dataSource.map(data => {
      println("ftp://" + data.getAs[String](hostField) + ":2121" + data.getAs[String](spicField))
      Data(data.getAs[Long](idField),
        data.getAs[Timestamp](timeField),
        data.getAs[String](spicField).substring(1, data.getAs[String](spicField).indexOf("/", 1)),
        data.getAs[String](hostField), "ftp://" +
          data.getAs[String](hostField) + ":2121" +
          data.getAs[String](spicField), "ftp://" +
          data.getAs[String](hostField) + ":2121" +
          data.getAs[String](bpicField))
    }).createOrReplaceTempView("mysqlTable")

    val joinData = spark.sql("select T1.feature, T2.* from parquetTable as T1 inner join mysqlTable as T2 on T1.ftpurl=T2.spic")

    val idPointRDD = joinData.rdd.map(data =>
      (data.getAs[String]("spic"),
        Vectors.dense(data.getAs[mutable.WrappedArray[Float]]("feature")
          .toArray.map(_.toDouble))))
      .cache()
    val centerList = new util.ArrayList[CenterData]()
    val kMeansModel = KMeans.train(idPointRDD.map(data => data._2), numClusters, numIterations)
    kMeansModel.clusterCenters.foreach(x => {
      println("Center Point of Cluster" + clusterIndex + ":")
      println(x)
      centerList.add(new CenterData(clusterIndex, x.toArray))
      clusterIndex += 1
    })
    val centerListTmp = new util.ArrayList[CenterData]()
    centerListTmp.addAll(centerList)
    val deleteCenter = new util.ArrayList[Int]()
    val union_center = new util.HashMap[Int, util.ArrayList[Int]]
    for (i <- 0 to centerListTmp.size() - 1) {
      val first = centerListTmp.get(i)
      if (!deleteCenter.contains(first.num)) {
        val centerSimilarity = new util.ArrayList[Int]()
        val iter = centerList.iterator()
        while (iter.hasNext) {
          val second = iter.next()
          val pairSim = cosineMeasure(first.data, second.data)
          if (pairSim > similarityThreshold) {
            deleteCenter.add(second.num)
            centerSimilarity.add(second.num)
            iter.remove()
          }
        }
        union_center.put(first.num, centerSimilarity)
      }
    }

    /* val unionCenterMap = new mutable.HashMap
     for (i <- 0 to centerSimilarity.size() - 1) {
       println(centerSimilarity.get(i))
     }*/

    val dataCenter = idPointRDD.map(_._2).map(p => (kMeansModel.predict(p), kMeansModel.clusterCenters.apply(kMeansModel.predict(p)), p))

    val zipDataCenter = dataCenter.zip(idPointRDD.map(_._2))
    val point_center_dist = zipDataCenter.map(data => (data._1._1, cosineMeasure(data._1._2.toArray, data._2.toArray)))
    val viewData = joinData.select("id", "time", "ipc", "host", "spic", "bpic", "feature").rdd
    val predictResult = point_center_dist.zip(viewData).groupBy(key => key._1._1).mapValues(f => {
      f.toList.filter(data => data._1._2 > similarityThreshold).sortWith((a, b) => (a._1._2 < b._1._2))
    })
    //.filter(data => data._2.length > appearCount)
    val table1List = new util.ArrayList[ClusteringAttribute]()
    val uuidString = UUID.randomUUID().toString

    /* val center_Point_map = mutable.HashMap[Int, mutable.WrappedArray[Float]]()
     val firstImgRDD = predictResult.map(data =>
       (data._1, data._2(0)._2.getAs[mutable.WrappedArray[Float]]("feature")))

     val zip_firstImgRDD = firstImgRDD.zipWithIndex()
     val join_zipedfirstImgRDD = zip_firstImgRDD.cartesian(zip_firstImgRDD).filter(f => f._1._2 < f._2._2)
     join_zipedfirstImgRDD.map(data=>
       (data._1._1._1,data._2._1._1,cosineMeasure(data._1._1._2.toArray.map(_.toDouble),data._2._1._2.toArray.map(_.toDouble))))
       .foreach(println(_))

     val indexed = IndexedRDD(predictResult).cache()*/
    predictResult.map(data => (data._1, data._2)).sortByKey().collect().foreach(println(_))
    val indexedResult = IndexedRDD(predictResult).cache()
    indexedResult.get(10)

    /*val iter_center = union_center.keySet().iterator()
    while (iter_center.hasNext) {
      val key = iter_center.next()
      val value = union_center.get(key)
      val value_iter = value.iterator()
      while (value_iter.hasNext) {
        val first_list=indexedResult.get(key).get
        val cluster_tmp = value_iter.next()
        val second_list=indexedResult.get(cluster_tmp).get
        indexedResult.put(key,first_list.union(second_list))
      }
    }*/

    /*predictResult.map(data => {
      val attribute = new ClusteringAttribute()
      attribute.setClusteringId(data._1.toString + uuidString)
      attribute.setCount(data._2.length)
      attribute.setLastAppearTime(data._2(0)._2.getTimestamp(1).toString)
      attribute.setLastIpcId(data._2(0)._2.getAs[String]("ipc"))
      attribute.setFirstAppearTime(data._2(data._2.length - 1)._2.getTimestamp(1).toString)
      attribute.setFirstIpcId(data._2(data._2.length - 1)._2.getAs[String]("ipc"))
      attribute.setFtpUrl(data._2(0)._2.getAs[String]("spic"))
      attribute
    }).collect().foreach(data => table1List.add(data))

    val mon = calendar.get(Calendar.MONTH)
    var monStr = ""
    if (mon < 10) {
      monStr = "0" + mon
    } else {
      monStr = String.valueOf(mon)
    }
    val yearMon = calendar.get(Calendar.YEAR) + "-" + monStr
    LOG.info("write clustering info to HBase...")
    PutDataToHBase.putClusteringInfo(yearMon, table1List)


    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val putDataToEs = PutDataToEs.getInstance()
    predictResult.foreach(data => {
      val rowKey = yearMon + "-" + data._1 + uuidString
      println(rowKey)
      data._2.foreach(p => {
        val date = new Date(p._2.getAs[Timestamp]("time").getTime)
        val dateNew = sdf.format(date)
        val status = putDataToEs.upDateDataToEs(p._2.getAs[String]("spic"), rowKey, dateNew, p._2.getAs[Long]("id").toInt)
        if (status != 200) {
          LOG.info("Put data to es failed! And the failed ftpurl is " + p._2.getAs("spic"))
        }
      })
    }) */
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
    0.5 + 0.5 * (member / denominator)
  }
}

