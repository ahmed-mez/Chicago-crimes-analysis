
object Cells {
  import org.apache.spark._
  import notebook._

  /* ... new cell ... */

  object CrimeAnalyserByWards {
    def CrimesByWards() :org.apache.spark.sql.DataFrame = {
      
      val session = SparkSession.builder().appName("Crime analyser by ward 1").master("local").getOrCreate()
      val data1 = session.read.format("csv").option("header", "true").load("/crimes.csv")
      val data = data1.na.drop()
      
      val wards = data.select("Ward").rdd
      
      val mostDangerousWards = wards.map{ case ward => (ward(0).toString(), 1) }.reduceByKey(_ + _).sortBy(-_._2)
        .toDF("Ward", "Crimes")
                                                                                           
      val locations: Map[String, (Double, Double)] = Map()
      
      val columnNames = Seq("Ward", "Latitude", "Longitude")
      val wardLongLat = data.select(columnNames.head, columnNames.tail: _*).rdd
      
      val wardLongLatReduce = wardLongLat
          .map{row => (row.getString(0), (row.getString(1).toDouble, row.getString(2).toDouble))}
          .reduceByKey((a, b) => ( (a._1+ b._1)/2 , (a._2 + b._2)/2))
      
      val listWard = wardLongLatReduce.map(row => row._1)
      val listLong = wardLongLatReduce.map(row => row._2._1)
      val listLat = wardLongLatReduce.map(row => row._2._2)
      
      val listLatLong = listLat.zip(listLong).toDF("Latitude", "Longitude")
      val listLatWard = listLat.zip(listWard).toDF("Latitude", "Ward")
      
      val result = listLatLong.join(listLatWard, Seq("Latitude"), "inner")
      
      val finalResult = result.join(mostDangerousWards, Seq("Ward"), "inner")
      
      return finalResult
    }
  }

  /* ... new cell ... */

  val proccessedData1 = CrimeAnalyserByWards.CrimesByWards()
  // proccessedData1.show()
  val chart1 = GeoPointsChart(proccessedData1, latLonFields = Some(("Longitude", "Latitude")),
                         rField = Some("Crimes"), colorField = Some("Crimes"), sizes = (1000, 1000))
  chart1

  /* ... new cell ... */

  object CrimeAnalyserByTime {
    def CrimesByTime() :Array[org.apache.spark.sql.DataFrame] = {
      
      val session = SparkSession.builder().appName("Crime analyser by time").master("local").getOrCreate()
      val data1 = session.read.format("csv").option("header", "true").load("/crimes.csv")
      val data = data1.drop()
      
      val dates = data.select("Date").toDF("Date")
      val times = dates.map(date => date.getString(0).split(" ")).map(date => date(1)
                                                               .split(":")(0) + " " + date(2))
      val months = dates.map(date => date.getString(0).split(" ")(0).split("/")(0)).rdd
      val years = dates.map(date => date.getString(0).split(" ")(0).split("/")(2)).rdd
      
      val convertTime = (line:String) => { if(line.split(" ")(1) == "PM") (line.split(" ")(0).toInt + 12)
                                        else line.split(" ")(0).toInt }
      
      val timesInt = times.map(convertTime).rdd
      
      val crimesByHour = timesInt.map(hour => (hour, 1)).reduceByKey(_ + _).sortBy(-_._2).toDF("Hour", "Crimes")
      val crimesByMonth = months.map(month => (month, 1)).reduceByKey(_ + _).sortBy(_._1).toDF("Month", "Crimes")
      val crimesByYear = years.map(year => (year, 1)).reduceByKey(_ + _).sortBy(_._1).toDF("Year", "Crimes")
          
      return Array(crimesByHour, crimesByMonth, crimesByYear)
    }
  }

  /* ... new cell ... */

  val proccessedData2 = CrimeAnalyserByTime.CrimesByTime()(0)
  // proccessedData2.show()
  val chart2 = PivotChart(proccessedData2)
  chart2

  /* ... new cell ... */

  val proccessedData4 = CrimeAnalyserByTime.CrimesByTime()(1)
  // proccessedData4.show()
  val chart4 = PivotChart(proccessedData4)
  chart4

  /* ... new cell ... */

  val proccessedData5 = CrimeAnalyserByTime.CrimesByTime()(2)
  // proccessedData5.show()
  val chart5 = PivotChart(proccessedData5)
  chart5

  /* ... new cell ... */

  object CrimeAnalyserBeatEfficency {
    def BeatEfficency() :org.apache.spark.sql.DataFrame = {
      
      val session = SparkSession.builder().appName("Efficency of each beat in Chicago").master("local").getOrCreate()
      val data = session.read.format("csv").option("header", "true").load("/crimes.csv")
  
      val arrestString = data.select("Arrest").map(row => row.getString(0))
      val arrestInt = arrestString.map(arrest => { if(arrest == "true") 1 else 0 } ).rdd
  
      val beat = data.select("Beat").map(row => row.getString(0)).rdd
      val arrestByBeat = beat.zip(arrestInt)
      val nbArrestByBeat = arrestByBeat.reduceByKey(_ + _).toDF("Beat", "Arrest")
      val totalByBeat = beat.map(beat => (beat, 1)).reduceByKey(_ + _).toDF("Beat", "Total crimes")
      
      val beatArrestTotal = nbArrestByBeat.join(totalByBeat, Seq("Beat"), "inner").orderBy(desc("Arrest"))
      
      val beatRate = beatArrestTotal
      .map(row => (row.getString(0), row.getInt(1), row.getInt(2), (row.getInt(1)*100/row.getInt(2)).toInt))
      .toDF("Beat", "Arrest", "Total crimes", "Rate")
      
      return beatRate
    }
  }

  /* ... new cell ... */

  val proccessedData3 = CrimeAnalyserBeatEfficency.BeatEfficency()
  // proccessedData3.show()
  val chart3 = BarChart(proccessedData3, fields = Some("Beat", "Rate"), sizes = (800,600))
  chart3

  /* ... new cell ... */

  object DistrictEfficency {
    def DistrictEfficency() :org.apache.spark.sql.DataFrame = {
      
      val session = SparkSession.builder().appName("Efficency of each district in Chicago").master("local").getOrCreate()
      val data = session.read.format("csv").option("header", "true").load("/crimes.csv")
  
      val arrestString = data.select("Arrest").map(row => row.getString(0))
      val arrestInt = arrestString.map(arrest => { if(arrest == "true") 1 else 0 } ).rdd
  
      val district = data.select("District").map(row => row.getString(0)).rdd
      val arrestByDistrict = district.zip(arrestInt)
      val nbArrestByDistrict = arrestByDistrict.reduceByKey(_ + _).toDF("District", "Arrest")
      val totalByDistrict = district.map(district => (district, 1)).reduceByKey(_ + _).toDF("District", "Total crimes")
      
      val districtArrestTotal = nbArrestByDistrict.join(totalByDistrict, Seq("District"), "inner").orderBy(desc("Arrest"))
      
      val districtRate = districtArrestTotal
      .map(row => (row.getString(0), row.getInt(1), row.getInt(2), (row.getInt(1)*100/row.getInt(2)).toInt))
      .toDF("District", "Arrest", "Total crimes", "Rate")
      
      return districtRate
    }
  }

  /* ... new cell ... */

  val proccessedData6 = DistrictEfficency.DistrictEfficency()
  // proccessedData6.show()
  val chart6 = BarChart(proccessedData6, fields = Some("District", "Rate"), sizes = (800,600))
  chart6

  /* ... new cell ... */

  object WardEfficency {
    def WardEfficency() :org.apache.spark.sql.DataFrame = {
      
      val session = SparkSession.builder().appName("Efficency of each ward in Chicago").master("local").getOrCreate()
      val data = session.read.format("csv").option("header", "true").load("/crimes.csv")
  
      val arrestString = data.select("Arrest").map(row => row.getString(0))
      val arrestInt = arrestString.map(arrest => { if(arrest == "true") 1 else 0 } ).rdd
  
      val ward = data.select("Ward").map(row => row.getString(0)).rdd
      val arrestByWard = ward.zip(arrestInt)
      val nbArrestByWard = arrestByWard.reduceByKey(_ + _).toDF("Ward", "Arrest")
      val totalByWard = ward.map(ward => (ward, 1)).reduceByKey(_ + _).toDF("Ward", "Total crimes")
      
      val wardArrestTotal = nbArrestByWard.join(totalByWard, Seq("Ward"), "inner").orderBy(desc("Arrest"))
      
      val wardRate = wardArrestTotal
      .map(row => (row.getString(0), row.getInt(1), row.getInt(2), (row.getInt(1)*100/row.getInt(2)).toInt))
      .toDF("Ward", "Arrest", "Total crimes", "Rate")
      
      return wardRate
    }
  }

  /* ... new cell ... */

  val proccessedData7 = WardEfficency.WardEfficency()
  // proccessedData7.show()
  val chart7 = BarChart(proccessedData7, fields = Some("Ward", "Rate"), sizes = (800,600))
  chart7

  /* ... new cell ... */
}
                  