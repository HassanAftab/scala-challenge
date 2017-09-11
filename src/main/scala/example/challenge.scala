package example
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame,SQLContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Hello extends fileImport with App {
  println(greeting)
  val spark = SparkSession.builder().master("local").appName("TV Series Analysis").config("spark.sql.warehouse.dir","file:///tmp/spark-warehouse").getOrCreate()
  //set new runtime options
  //spark.conf.set("spark.sql.shuffle.partitions", 6)
  //spark.conf.set("spark.executor.memory", "2g")
  import spark.implicits._
  var anime = spark.read.option("header", true).csv("src/test/resources/data/anime.csv")
  var ratings = spark.read.option("header", true).csv("src/test/resources/data/rating.csv")

/*
	Top 10 Ratings 
	anime.select("*").orderBy($"rating".desc).limit(10).show()
*/

/*
	First filter on all TV Series 
	anime.select($"rating").filter("type == 'TV'").orderBy($"rating".desc).limit(10).show()
*/


/*
	Filter on all TV Series and Episodes > 10
	Top 10 ratings
	
	anime.select("*").filter("type == 'TV' AND episodes > 10 ").orderBy($"rating".desc).limit(10).show()
*/


/* 
  Filter on all TV Series and Episodes > 10
	GroupBy based on GENRE, Aggregate ratings average
	Top 10 ratings
*/

//  Final Query

  val top10TvSeries = anime.filter("type =='TV' AND episodes > 10")
                      .groupBy($"genre").agg(avg("rating").as("averageRating"))
                      .orderBy($"averageRating".desc)
                      .limit(10)
  top10TvSeries.show()

/////////////////////////// Data Join  ///////////////////////////

// Join of Rating.csv and Anime.csv 
/*
  val output = anime.join(ratings,Seq("anime_id"),joinType="inner")

  val joinedtop10TvSeries = output.filter("type =='TV' AND episodes > 10")
                      .groupBy($"genre").agg(avg("rating").as("averageRating"))
                      .orderBy($"averageRating".desc)
                      .limit(10)

  output.show(10)
*/
}

trait fileImport {
  lazy val greeting: String = "hello"
}
