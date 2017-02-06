/**
  * Created by jerome on 06/02/17.
  */
import scala.io.Source
import au.com.bytecode.opencsv.CSVWriter
import java.io.FileWriter
import java.io.BufferedWriter

import scala.collection.mutable

object ETL {
  def main(args: Array[String]): Unit = {

    val out = new BufferedWriter(new FileWriter("beaufichier.csv"));
    val writer = new CSVWriter(out, ';')
    val filename = "train.csv"

    val listDayOfWeek = mutable.ListBuffer[String]()
    val listDate = mutable.ListBuffer[String]()
    val listCategory = mutable.ListBuffer[String]()
    val listDistrict = mutable.ListBuffer[String]()
    val listResolution = mutable.ListBuffer[String]()
    val listX = mutable.ListBuffer[String]()
    val listY = mutable.ListBuffer[String]()

    for (line <- Source.fromFile(filename).getLines) {
        val cols = line.split(";").map(_.trim)
        // val ligne = Array({cols(0)}, {cols(1)}, {cols(3)}, {cols(4)}, {cols(5)}, {cols(7)}, {cols(8)})
        // writer.writeNext(ligne)
        //transformDate(listDate, {cols(0)})
        transformCategory(listCategory, {
          cols(1)
        })
        transformDayOfWeek(listDayOfWeek, {
          cols(3)
        })
        transformDistrict(listDistrict, {
          cols(4)
        })
        transformResolution(listResolution, {
          cols(5)
        })
        // transformX(listX, {cols(7)})
        // transformY(listY, {cols(8)})
    }


    /*
    val chunkSize = 128 * 1024
    val iterator = Source.fromFile(filename).getLines.grouped(chunkSize)
    iterator.foreach { lines =>
      lines.par.map(line => {
        val cols = line.split(";").map(_.trim)

        val listDayOfWeek = mutable.ListBuffer[String]()
        val listCategory = mutable.ListBuffer[String]()
        val listDistrict = mutable.ListBuffer[String]()
        val listResolution = mutable.ListBuffer[String]()
        transformCategory(listCategory, {cols(1)})
        transformDayOfWeek(listDayOfWeek, {cols(3)})
        transformDistrict(listDistrict, {cols(4)})
        transformResolution(listResolution, {cols(5)})
        new Tuple4[mutable.ListBuffer[String], mutable.ListBuffer[String]]()listCategory, listDayOfWeek, listDistrict, listResolution)
      })
      }
    */

    writer.close()
    //printList("DATE", listDate)
    printList("CATEGORY", listCategory)
    printList("DAYOFWEEK", listDayOfWeek)
    printList("DISTRICT", listDistrict)
    printList("RESOLUTION", listResolution)
    //printList("X", listX)
    //printList("Y", listY)
  }

  def transformDate(listDate : mutable.ListBuffer[String], date : String){
    if(!listDate.contains(date)){
      listDate.++=(List(date))
    }
  }

  def transformCategory(listCategory : mutable.ListBuffer[String], category : String){
    if(!listCategory.contains(category)){
      listCategory.++=(List(category))
    }
  }

  def transformDayOfWeek(listDayOfWeek : mutable.ListBuffer[String], dayOfWeek : String) = {
    if(!listDayOfWeek.contains(dayOfWeek)){
      listDayOfWeek.++=(List(dayOfWeek))
    }
  }

  def transformDistrict(listDistrict : mutable.ListBuffer[String], district : String){
    if(!listDistrict.contains(district)){
      listDistrict.++=(List(district))
    }
  }

  def transformResolution(listResolution : mutable.ListBuffer[String], resolution : String){
    if(!listResolution.contains(resolution)){
      listResolution.++=(List(resolution))
    }
  }

  def transformX(listX : mutable.ListBuffer[String], x : String){
    if(!listX.contains(x)){
      listX.++=(List(x))
    }
  }

  def transformY(listY : mutable.ListBuffer[String], y  : String){
    if(!listY.contains(y)){
      listY.++=(List(y))
    }
  }

  def printList(title : String, list : mutable.ListBuffer[String]): Unit ={
    println(title+"\n")
    println("Size : "+list.size)
    list.foreach(println)
    println("\n\n\n\n\n")
  }
}
