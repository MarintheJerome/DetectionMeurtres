/**
  * Created by jerome on 06/02/17.
  */
import scala.io.Source
import au.com.bytecode.opencsv.CSVWriter
import java.io.FileWriter
import java.io.BufferedWriter

object ETL {
  def main(args: Array[String]): Unit = {

    val out = new BufferedWriter(new FileWriter("beaufichier.txt"))
    val writer = new CSVWriter(out, ',')
    val filename = "train.csv"

    /*val ligne = Array("0 - Jour", "1 - Heures",
      "2 - Category", "3 - Description",
      "4 - DayOfWeek : Monday", "5 - DayOfWeek : Tuesday", "6- DayOfWeek : Wednesday",
      "7- DayOfWeek : Thursday", "8 - DayOfWeek : Friday", "9 - DayOfWeek : Saturday", "10 - DayOfWeek : Sunday",
      "11 - District", "12 - Resolution : None", "13 - Resolution : Arreted, booked", "14 - Resolution : Arrested, Cited",
      "15 - Resolution : Others")
    writer.writeNext(ligne)*/

    for (line <- Source.fromFile(filename).getLines.drop(1)) {
      val cols = line.split(";").map(_.trim)

      val categories = categoryInBinary({cols(1)})
      val daysOfWeek = dayInBinary({cols(3)})
      val district = districtInBinary({cols(4)})
      val resolution = resolutionInBinary({cols(5)})
      val dates = {cols(0)}.split(" +" )
      val hours = hourInBinary({dates(1)})
      /* val ligne = Array(
        district,
        "1:"+{hours(0)}, "2:"+{hours(1)}, "3:"+{hours(2)}, "4:"+{hours(3)},
        "5:"+{categories(0)}, "6:"+{categories(1)}, "7:"+{categories(2)}, "8:"+{categories(3)}, "9:"+{categories(4)},
        "10:"+{categories(5)}, "11:"+{categories(6)}, "12:"+{categories(7)}, "13:"+{categories(8)}, "14:"+{categories(9)},
        "15:"+{categories(10)}, "16:"+{categories(11)}, "17:"+{categories(12)}, "18:"+{categories(13)},
        "19:"+{daysOfWeek(0)}, "20:"+{daysOfWeek(1)}, "21:"+{daysOfWeek(2)}, "22:"+{daysOfWeek(3)}, "23:"+{daysOfWeek(4)}, "24:"+{daysOfWeek(5)}, "25:"+{daysOfWeek(6)},
        "26:"+{resolution(0)}, "27:"+{resolution(1)}, "28:"+{resolution(2)}, "29:"+{resolution(3)}
        )*/
      val ligne = district+" 1:"+{hours(0)}+" 2:"+{hours(1)}+" 3:"+{hours(2)}+" 4:"+{hours(3)}+
        " 5:"+{categories(0)}+" 6:"+{categories(1)}+" 7:"+{categories(2)}+" 8:"+{categories(3)}+" 9:"+{categories(4)}+
        " 10:"+{categories(5)}+" 11:"+{categories(6)}+" 12:"+{categories(7)}+" 13:"+{categories(8)}+" 14:"+{categories(9)}+
        " 15:"+{categories(10)}+" 16:"+{categories(11)}+" 17:"+{categories(12)}+" 18:"+{categories(13)}+
        " 19:"+{daysOfWeek(0)}+" 20:"+{daysOfWeek(1)}+" 21:"+{daysOfWeek(2)}+" 22:"+{daysOfWeek(3)}+" 23:"+{daysOfWeek(4)}+" 24:"+{daysOfWeek(5)}+" 25:"+{daysOfWeek(6)}+
        " 26:"+{resolution(0)}+" 27:"+{resolution(1)}+" 28:"+{resolution(2)}+" 29:"+{resolution(3)}+"\n"
      out.write(ligne)
    }
    writer.close()
  }

  // Chaque colonne représente un jour
  def dayInBinary(day : String) : Array[String] = {
    day match {
      case "Monday" => Array("1", "0", "0","0","0","0","0")
      case "Tuesday" => Array("0", "1", "0","0","0","0","0")
      case "Wednesday" => Array("0", "0", "1","0","0","0","0")
      case "Thursday" => Array("0", "0", "0","1","0","0","0")
      case "Friday" => Array("0", "0", "0","0","1","0","0")
      case "Saturday" => Array("0", "0", "0","0","0","1","0")
      case "Sunday" => Array("0", "0", "0","0","0","0","1")
    }
  }

  def districtInBinary(district : String) : String = {
    district match {
      case "NORTHERN" => "0"
      case "PARK" => "1"
      case "INGLESIDE" => "2"
      case "BAYVIEW" => "3"
      case "RICHMOND" => "4"
      case "CENTRAL" => "5"
      case "TARAVAL" => "6"
      case "TENDERLOIN" => "7"
      case "SOUTHERN" => "8"
      case "MISSION" => "9"
    }
  }

  // Chaque colonne représente une résolution
  def resolutionInBinary(resolution : String) : Array[String] = {
    resolution match {
      case "NONE" => Array("1", "0", "0", "0")
      case "ARREST, BOOKED" => Array("0", "1", "0", "0")
      case "ARREST, CITED" => Array("0", "0", "1", "0")
      case "PSYCHOPATHIC CASE" => Array("0", "0", "0", "1")
      case "JUVENILE BOOKED" => Array("0", "0", "0", "1")
      case "UNFOUNDED" => Array("0", "0", "0", "1")
      case "EXCEPTIONAL CLEARANCE" => Array("0", "0", "0", "1")
      case "LOCATED" => Array("0", "0", "0", "1")
      case "CLEARED-CONTACT JUVENILE FOR MORE INFO" => Array("0", "0", "0", "1")
      case "NOT PROSECUTED" => Array("0", "0", "0", "1")
      case "JUVENILE DIVERTED" => Array("0", "0", "0", "1")
      case "COMPLAINANT REFUSES TO PROSECUTE" => Array("0", "0", "0", "1")
      case "JUVENILE ADMONISHED" => Array("0", "0", "0", "1")
      case "JUVENILE CITED" => Array("0", "0", "0", "1")
      case "DISTRICT ATTORNEY REFUSES TO PROSECUTE" => Array("0", "0", "0", "1")
      case "PROSECUTED BY OUTSIDE AGENCY" => Array("0", "0", "0", "1")
      case "PROSECUTED FOR LESSER OFFENSE" => Array("0", "0", "0", "1")
    }
  }

  // Les 13 premières colonnes représentent les 13 categories les plus fréquentes, la 14 ème le reste des catégories
  def categoryInBinary(category : String) : Array[String] ={
    category match{
      case "LARCENY/THEFT" => Array("1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")
      case "OTHER OFFENSES" => Array("0", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")
      case "NON-CRIMINAL" => Array("0", "0", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")
      case "ASSAULT" => Array("0", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")
      case "DRUG/NARCOTIC" => Array("0", "0", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0")
      case "VEHICLE THEFT" => Array("0", "0", "0", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0", "0")
      case "VANDALISM" => Array("0", "0", "0", "0", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0")
      case "WARRANTS" => Array("0", "0", "0", "0", "0", "0", "0", "1", "0", "0", "0", "0", "0", "0")
      case "BURGLARY" => Array("0", "0", "0", "0", "0", "0", "0", "0", "1", "0", "0", "0", "0", "0")
      case "SUSPICIOUS OCC" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "1", "0", "0", "0", "0")
      case "MISSING PERSON" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1", "0", "0", "0")
      case "ROBBERY" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1", "0", "0")
      case "FRAUD" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1", "0")
      case "FORGERY/COUNTERFEITING" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "SECONDARY CODES" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "WEAPON LAWS" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "PROSTITUTION" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "TRESPASS" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "STOLEN PROPERTY" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "SEX OFFENSES FORCIBLE" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "DISORDERLY CONDUCT" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "DRUNKENNESS" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "RECOVERED VEHICLE" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "KIDNAPPING" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "DRIVING UNDER THE INFLUENCE" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "RUNAWAY" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "LIQUOR LAWS" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "ARSON" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "LOITERING" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "EMBEZZLEMENT" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "SUICIDE" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "FAMILY OFFENSES" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "BAD CHECKS" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "BRIBERY" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "EXTORTION" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "SEX OFFENSES NON FORCIBLE" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "GAMBLING" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "PORNOGRAPHY/OBSCENE MAT" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
      case "TREA" => Array("0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1")
    }
  }

  def hourInBinary(hour : String) : Array[String] = {
    val hourInt = hour.split(":")(0).toInt
    if(hourInt >= 0 && hourInt < 6){
      return Array("1", "0", "0", "0")
    }
    if(hourInt >= 6 && hourInt < 12){
      return Array("0", "1", "0", "0")
    }
    if(hourInt >= 12 && hourInt < 18){
      return Array("0", "0", "1", "0")
    }
    if(hourInt >= 18 && hourInt < 24){
      return Array("0", "0", "0", "1")
    }
    Array("0", "0", "0", "0")
  }
}
