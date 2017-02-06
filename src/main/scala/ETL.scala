/**
  * Created by jerome on 06/02/17.
  */
import scala.io.Source
import au.com.bytecode.opencsv.CSVWriter
import java.io.FileWriter
import java.io.BufferedWriter

object ETL {
  def main(args: Array[String]): Unit = {

    val out = new BufferedWriter(new FileWriter("beaufichier.csv"));
    val writer = new CSVWriter(out, ';')
    val filename = "train.csv"

    for (line <- Source.fromFile(filename).getLines()) {
      val cols = line.split(";").map(_.trim)
      // do whatever you want with the columns here
      val ligne = Array({cols(0)}, {cols(1)}, {cols(3)}, {cols(4)}, {cols(5)}, {cols(7)}, {cols(8)})
      writer.writeNext(ligne)
    }
    writer.close()
  }

  def transformDate(date : String): String = {
    date
  }

  def transformCategory(category : String): String = {
    category
  }

  def transformDayOfWeek(dayOfWeek : String): String = {
    dayOfWeek
  }

  def transformDistrict(district : String): String = {
    district
  }

  def transformResolution(resolution : String): String = {
    resolution
  }

  def transformX(x : String): String = {
    x
  }

  def transformY(y : String): String = {
    y
  }
}
