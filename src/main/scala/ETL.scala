/**
  * Created by jerome on 06/02/17.
  */
import scala.io.Source
import java.io.FileWriter
import java.io.BufferedWriter

object ETL {
  def main(args: Array[String]): Unit = {

    val outDistrict = new BufferedWriter(new FileWriter("District.txt"))
    val outCategory = new BufferedWriter(new FileWriter("Category.txt"))
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

      val category = categoryToNumber({cols(1)})
      val categories = categoryInBinary({cols(1)})
      val daysOfWeek = dayInBinary({cols(3)})
      val district = districtToNumber({cols(4)})
      val districts = districtToBinary({cols(4)})
      val resolution = resolutionInBinary({cols(5)})
      val dates = {cols(0)}.split(" +" )
      val hours = hourInBinary({dates(1)})
      val ligneDistrict = district+
        " 1:"+{categories(0)}+" 2:"+{categories(1)}+" 3:"+{categories(2)}+" 4:"+{categories(3)}+" 5:"+{categories(4)}+
        " 6:"+{categories(5)}+" 7:"+{categories(6)}+" 8:"+{categories(7)}+" 9:"+{categories(8)}+" 10:"+{categories(9)}+
        " 11:"+{categories(10)}+" 12:"+{categories(11)}+" 13:"+{categories(12)}+" 14:"+{categories(13)}+
        " 15:"+{daysOfWeek(0)}+" 16:"+{daysOfWeek(1)}+" 17:"+{daysOfWeek(2)}+" 18:"+{daysOfWeek(3)}+" 19:"+{daysOfWeek(4)}+" 20:"+{daysOfWeek(5)}+" 21:"+{daysOfWeek(6)}+
        " 22:"+{resolution(0)}+" 23:"+{resolution(1)}+" 24:"+{resolution(2)}+" 25:"+{resolution(3)}+"\n"
      val ligneCategory = category+
        " 1:"+{daysOfWeek(0)}+" 2:"+{daysOfWeek(1)}+" 3:"+{daysOfWeek(2)}+" 4:"+{daysOfWeek(3)}+" 5:"+{daysOfWeek(4)}+" 6:"+{daysOfWeek(5)}+" 7:"+{daysOfWeek(6)}+
        " 8:"+{districts(0)}+" 9:"+{districts(1)}+" 10:"+{districts(2)}+" 11:"+{districts(3)}+" 12:"+{districts(4)}+
        " 13:"+{districts(5)}+" 14:"+{districts(6)}+" 15:"+{districts(7)}+" 16:"+{districts(8)}+" 17:"+{districts(9)}+
        " 18:"+{resolution(0)}+" 19:"+{resolution(1)}+" 20:"+{resolution(2)}+" 21:"+{resolution(3)}+"\n"
      outDistrict.write(ligneDistrict)
      outCategory.write(ligneCategory)
    }
    outDistrict.close()
    outCategory.close()
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

  def districtToNumber(district : String) : String = {
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

  def categoryToNumber(category : String) : String = {
    category match{
      case "LARCENY/THEFT" => "0"
      case "OTHER OFFENSES" => "1"
      case "NON-CRIMINAL" => "2"
      case "ASSAULT" => "3"
      case "DRUG/NARCOTIC" => "4"
      case "VEHICLE THEFT" => "5"
      case "VANDALISM" => "6"
      case "WARRANTS" => "7"
      case "BURGLARY" => "8"
      case "SUSPICIOUS OCC" => "9"
      case "MISSING PERSON" => "10"
      case "ROBBERY" => "11"
      case "FRAUD" => "12"
      case "FORGERY/COUNTERFEITING" => "13"
      case "SECONDARY CODES" => "13"
      case "WEAPON LAWS" => "13"
      case "PROSTITUTION" => "13"
      case "TRESPASS" => "13"
      case "STOLEN PROPERTY" => "13"
      case "SEX OFFENSES FORCIBLE" => "13"
      case "DISORDERLY CONDUCT" => "13"
      case "DRUNKENNESS" => "13"
      case "RECOVERED VEHICLE" => "13"
      case "KIDNAPPING" => "13"
      case "DRIVING UNDER THE INFLUENCE" => "13"
      case "RUNAWAY" => "13"
      case "LIQUOR LAWS" => "13"
      case "ARSON" => "13"
      case "LOITERING" => "13"
      case "EMBEZZLEMENT" => "13"
      case "SUICIDE" => "13"
      case "FAMILY OFFENSES" => "13"
      case "BAD CHECKS" => "13"
      case "BRIBERY" => "13"
      case "EXTORTION" => "13"
      case "SEX OFFENSES NON FORCIBLE" => "13"
      case "GAMBLING" => "13"
      case "PORNOGRAPHY/OBSCENE MAT" => "13"
      case "TREA" => "13"
    }
  }

  def districtToBinary(district : String) : Array[String] = {
    district match{
      case "NORTHERN" => Array("1", "0", "0","0","0","0","0", "0", "0", "0")
      case "PARK" => Array("0", "1", "0","0","0","0","0", "0", "0", "0")
      case "INGLESIDE" => Array("0", "0", "1","0","0","0","0", "0", "0", "0")
      case "BAYVIEW" => Array("0", "0", "0","1","0","0","0", "0", "0", "0")
      case "RICHMOND" => Array("0", "0", "0","0","1","0","0", "0", "0", "0")
      case "CENTRAL" => Array("0", "0", "0","0","0","1","0", "0", "0", "0")
      case "TARAVAL" => Array("0", "0", "0","0","0","0","1", "0", "0", "0")
      case "TENDERLOIN" => Array("0", "0", "0","0","0","0","0", "1", "0", "0")
      case "SOUTHERN" => Array("0", "0", "0","0","0","0","0", "0", "1", "0")
      case "MISSION" => Array("0", "0", "0","0","0","0","0", "0", "0", "1")
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
