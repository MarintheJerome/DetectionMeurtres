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
        transfromValue(listCategory, {cols(1)})
        transfromValue(listDayOfWeek, {cols(3)})
        transfromValue(listDistrict, {cols(4)})
        transfromValue(listResolution, {cols(5)})
        // transformX(listX, {cols(7)})
        // transformY(listY, {cols(8)})
    }
    writer.close()
    //printList("DATE", listDate)
    printList("CATEGORY", listCategory)
    printList("DAYOFWEEK", listDayOfWeek)
    printList("DISTRICT", listDistrict)
    printList("RESOLUTION", listResolution)
    //printList("X", listX)
    //printList("Y", listY)
  }

  def transfromValue(list : mutable.ListBuffer[String], value : String): Unit ={
    if(!list.contains(value)){
      list.++=(List(value))
    }
  }

  def printList(title : String, list : mutable.ListBuffer[String]): Unit ={
    println(title+"\n")
    println("Size : "+list.size)
    list.foreach(println)
    println("\n\n\n\n\n")
  }
}
