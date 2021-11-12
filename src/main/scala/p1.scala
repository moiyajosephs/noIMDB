import scala.io.StdIn.readLine
import scala.io.StdIn.readInt
import java.sql.DriverManager
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level



object p1 {
  var logged_in = false
  var admin = false
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://localhost/p1project"
  val username = "root"
  val password = sys.env("JDBC_PASSWORD")
  var connection: Connection = null
  var session_user = "default"

  System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\hadoop") //spark session for windows
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("created spark session")
  spark.sparkContext.setLogLevel("ERROR")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

  /*ADMIN PORTION */
  def admin_menu(): Int = {
    println("Administrator: What would you like to do today?")
    println("1) Make new admin\n" +
      "2) Select query from databases\n" +
      "3) Update password\nEnter 1,2 or 3" + "\n4)Logout")
    val admin_choice = readInt()
    admin_choice
  }

  def convert_db(a:Int): String ={
    if(a==1){
      return "netflix1"
    }
    else if(a==2){
      return "disney1"
    }
    else if(a==3){
      return "hulu1"
    }
    else{
      return "Sorry we don't have that information"
    }
  }

  def make_new_user(answer: Int) = {
    //Make a new user based on user input
    println("Enter in your First Name")
    val firstName = readLine()
    println("Enter in your username, it must be unique")
    val username = readLine()
    println("Enter in your password, try to make it a good one")
    val password = readLine()

    (firstName, username, password)
  }



  def admin_queries(a: Int, db: String) = {
    /*"1)Count how many TV Shows" 2)Count how many Movies" 3) what is the most common genre
    4) count the number released in Fall/Spring/ Given month
    5) how much has been released since covid
    */
    if (a == 1) {
      spark.sql(s"SELECT COUNT(*) FROM $db where type = 'TV Show'").show()
      println("Do you want to save to a json file? 0 for no 1 for Yes")
      val x = readInt()
      x match {
        case 0 => "Okay"
        case 1 =>  {spark.sql(s"SELECT COUNT(*) FROM $db where type = 'TV Show'").write.format("org.apache.spark.sql.json").mode("overwrite").save(s"draft1/$db show")
        println(s"saved to $db show folder")
        }
        case _ => println("Not a valid input")
      }
    }
    else if(a == 2){
      spark.sql(s"SELECT COUNT(*) FROM $db where type = 'Movie'").show()
      println("Do you want to save to a json file? 0 for no 1 for Yes")
      val x = readInt()
      x match {
        case 0 => "Okay"
        case 1 =>  {spark.sql(s"SELECT COUNT(*) FROM $db where type = 'Movie'").write.format("org.apache.spark.sql.json").mode("overwrite").save(s"draft1/$db movie")
          println(s"saved to $db movie folder")
        }
        case _ => println("Not a valid input")
      }

    }
    else if(a==3){
      println("Now showing the top 3 genres in "+ db)
      spark.sql("DROP VIEW IF EXISTS common_genre")
      spark.sql(s"create view if not exists common_genre as (Select listed_in as `Genre`, count(listed_in) as `Count` from $db group by listed_in)")
      //spark.sql("select * from common_genre").show()
      spark.sql("select * from common_genre order by count desc").show(3, false)
      println("Do you want to save to a json file? 0 for no 1 for Yes")
      val x = readInt()
      x match {
        case 0 => "Okay"
        case 1 =>  {spark.sql("select * from common_genre order by count desc limit 3").write.format("org.apache.spark.sql.json").mode("overwrite").save(s"draft1/$db common genre")
          println(s"saved to $db common genre")
        }
        case _ => println("Not a valid input")
      }
    }
    else if(a==4){
      val view_name = s"$db"+"_view"
      println("Enter the year you want to see the content for")
      val query_year = readLine()
      println("Do you want to see TV Show or Movie")
      val query_type = readLine()
      spark.sql(s"create view if not exists $view_name as (select count(title) as `Count` from $db where type='$query_type' and release_year = $query_year)")
      spark.sql(s"select * from $view_name").show()
    }
    else if(a==5){
      val view_name = s"$db"+"_view"
      println("Enter the date_added you want to see the content for")
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val query_date = readLine()
      format.parse(query_date)
      println("Do you want to see TV Show or Movie")
      val query_type = readLine()
      spark.sql(s"create view if not exists $view_name as (select count(title) as `Count` from $db where type='$query_type' and date_added > '2020-10-10')")
      spark.sql(s"select * from $view_name").show()
    }
    else if(a == 6){
      println("Percentage in a given year. Enter your given year")
      val year = readInt()
      println("Do you want to see movies or Tv")
      val type_given = readLine()
      spark.sql(s"select round( ( ( (select count(title) as `Count` from $db where type='$type_given' and release_year = $year)/(SELECT count(*) FROM $db where type ='$type_given')) * 100),2) as `Percentage of $type_given` ").show()

    }
    else if(a ==7) {
      println("Enter the year you would like to know the projected percentage of")
      val year = readLine
      if (db == "netflix1") {
        spark.sql(s"select (($year-2004)/1.01) as `Expected TV Show percentage for $year`").show()
      }
      else if (db == "hulu1") {
        spark.sql(s"select (($year+2016)/0.306) as `Expected TV Show percentage for $year`").show()
      }
      else if (db == "disney1") {
        spark.sql(s"select (($year-2003)/1.75) as `Expected TV Show percentage for $year`").show()
      }
      else {
        println("not a valid database")
      }
    }
    else {
      println("Sorry that query didn't work :(")
    }


  }

  def admin_queries_compare(a:Int, db1:String, db2: String): Unit ={
    /*println("1) Which data base has more TV Shows?")
    println("2) Which database has more Movies?")
    println("3) Which dtabase has more content?")
    4) joins on common tv and movies
     */
    if (a == 1) {
      spark.sql(s"select COUNT(*) as `$db1 Count` FROM $db1 where type = 'TV Show'").show
      spark.sql(s"select COUNT(*) as `$db2 Count` FROM $db2 where type = 'TV Show'").show
      println("Do you want to save to a json file? 0 for no, 1 for yes")
      val x = readInt()
      x match {
        case 0 => "Okay"
        case 1 => {
          spark.sql(s"SELECT COUNT(*) as `$db1 Count` FROM $db1 where type = 'TV Show'").write.format("org.apache.spark.sql.json").mode("overwrite").save(s"draft1/$db1 $db2 show")
          spark.sql(s"select COUNT(*) as `$db2 Count` FROM $db2 where type = 'TV Show'").write.format("org.apache.spark.sql.json").mode("append").save(s"draft1/$db1 $db2 show")
        }
      }
    }
    else if(a == 2) {
      spark.sql(s"select COUNT(*) as `$db1 Count` FROM $db1 where type = 'Movie'").show
      spark.sql(s"select COUNT(*) as `$db2 Count` FROM $db2 where type = 'Movie'").show
      println("Do you want to save to a json file? 0 for no, 1 for yes")
      val x = readInt()
      x match {
        case 0 => "Okay"
        case 1 => {
          spark.sql(s"SELECT COUNT(*) as `$db1 Count` FROM $db1 where type = 'Movie'").write.format("org.apache.spark.sql.json").mode("overwrite").save(s"draft1/$db1 $db2 Movie")
          spark.sql(s"select COUNT(*) as `$db2 Count` FROM $db2 where type = 'Movie'").write.format("org.apache.spark.sql.json").mode("append").save(s"draft1/$db1 $db2 Movie")
        }
      }
    }
    else if(a==3){
      spark.sql(s"select COUNT(*) as `$db1 Count` FROM $db1").show
      spark.sql(s"select COUNT(*) as `$db2 Count` FROM $db2").show
    }
    else if(a==4){
        val joins= s"select $db1.title as `$db1`, $db2.title as `$db2`, $db1.date_added as `$db1 date`, $db2.date_added as `$db2 date` from $db1 join $db2 on $db1.title = $db2.title"
        spark.sql(joins).show(false)
    }
    else {
      println("Sorry that query didn't work :(")
    }
  }


  def make_admin(super_secret: String, session_user: String): Boolean = {
    //take in the super duper secret admin granting password
    var made_admin = false
    if (super_secret == "SUPER_SECRET") {
      try {
        val prpStmt2: PreparedStatement = connection.prepareStatement("UPDATE users " + s"SET administrator = 1 WHERE username = ?")
        prpStmt2.setString(1, session_user)
        prpStmt2.executeUpdate()
        prpStmt2.close
        made_admin = true
        println("Success new admin created!")
        return made_admin
      } catch {
        case e: Throwable => e.printStackTrace
          return false
      }
    } else {
      println("You entered the wrong super secret password, try again later")
      return made_admin
    }
  }

  /*---END OF ADMIN QUERIES ---*/


  /*---- USER FUNTION ------*/
  def search_database(db:String, title:String)={
    spark.sql(s"Select * from $db where title='$title'").show

  }
  def search_year(db:String, year:Int)={
    spark.sql(s"Select * from $db where release_year =$year").show
  }
  def search_director(db:String, director:String)={
    spark.sql(s"Select * from $db where director='$director'").show
  }
  def find_common(db1:String, db2:String)={
    val joins= s"select $db1.title as `$db1`, $db2.title as `$db2`, $db1.date_added as `$db1 date`, $db2.date_added as `$db2 date` from $db1 join $db2 on $db1.title = $db2.title"
    spark.sql(joins).show(false)
    println("Do you want to save to a json file? 0 for no 1 for Yes")
    val x = readInt()
    x match {
      case 0 => "Okay"
      case 1 =>  {spark.sql(joins).write.format("org.apache.spark.sql.json").mode("overwrite").save(s"usersave/$db1 $db2 common")
        println(s"saved to $db1 $db2 common")
      }
      case _ => println("Not a valid input")
    }
  }

  /*--------- END OF USER FUNCTION -----*/


  /*-- LOGIN FUNCTIONALITY --*/
  def login(username: String, password: String): Boolean = {
    val prpStmt2: PreparedStatement = connection.prepareStatement("SELECT passwordHash, administrator from users WHERE username = ?")
    prpStmt2.setString(1, username)
    val savedSet = prpStmt2.executeQuery()
    while (savedSet.next()) {
      val dbpassword = savedSet.getString("passwordHash")
      val dbadmin = savedSet.getInt("administrator")
      if(dbadmin == 1) {admin = true}
      if (dbpassword == password) {
        logged_in = true
        return logged_in
      }
      else {
        logged_in = false
      }
    }
    return false
  }

  def update_password(username: String, password: String): Boolean = {
    val prpStmt: PreparedStatement = connection.prepareStatement("SELECT passwordHash from users WHERE username = ?")
    prpStmt.setString(1, username)
    val savedSet = prpStmt.executeQuery()
    savedSet.next()
    val dbpassword = savedSet.getString("passwordHash")
    if (dbpassword == password) {
      println("Gotten from database" + dbpassword)
      println("Enter new password:")
      val new_password = readLine
      val prpStmt2: PreparedStatement = connection.prepareStatement("UPDATE users " + s"SET passwordHash = ? WHERE username = ?")
      prpStmt2.setString(1, new_password)
      prpStmt2.setString(2, username)
      prpStmt2.executeUpdate()
      prpStmt2.close
      return true
    }
    else {
      println("Failed to change password!")
      return false
    }
  }


  /*---- END OF LOGIN FUNCTIONALITY --*/



  /* -- CREATE DATABASES ---*/
  def create_disney()= {
    spark.sql("DROP table IF EXISTS disney1")
    spark.sql(sqlText = "create table IF NOT EXISTS disney1(show_id string, type string, title String, director array<String>, cast array<string>, country array<String>, date_added date, release_year int, rating String, duration String, listed_in String, description Array<string>) row format delimited fields terminated by ',' collection items terminated by '|' ")
    spark.sql("LOAD DATA LOCAL INPATH 'input/pl.csv' INTO TABLE disney1")
    spark.sql("SELECT * FROM disney1").show()
  }
  def create_hulu()={
    spark.sql("DROP table IF EXISTS hulu1")
    spark.sql(sqlText = "create table IF NOT EXISTS hulu1(show_id string, type string, title String, director array<String>, cast array<string>, country array<String>, date_added date, release_year int, rating String, duration String, listed_in String, description Array<string>) row format delimited fields terminated by ',' collection items terminated by '|' ")
    spark.sql("LOAD DATA LOCAL INPATH 'input/p2.csv' INTO TABLE hulu1")
    spark.sql("SELECT * FROM hulu1").show()
  }
  def create_netflix()={
    spark.sql("DROP table IF EXISTS netflix1")
    spark.sql(sqlText = "create table IF NOT EXISTS netflix1(show_id string, type string, title String, director array<String>, cast array<string>, country array<String>, date_added date, release_year int, rating String, duration String, listed_in String, description Array<string>) row format delimited fields terminated by ',' collection items terminated by '|' ")
    spark.sql("LOAD DATA LOCAL INPATH 'input/p3.csv' INTO TABLE netflix1")
    spark.sql("SELECT * FROM netflix1").show()
  }
  /*------- END OF MAKE DATABASES------*/

  def main(args: Array[String]): Unit = {
    println("Welcome to noIMDB where you can query data about Netflix, Disney+ and Hulu.\nWould you like to login, or create an account")
    println("Enter in 0 for new user, 1 for existing user.")
    val make_user = readInt

    try {

      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      if (make_user == 0) {
        val (f, u, p) = make_new_user(make_user)


        val insertsql = s"insert into users (administrator, firstName, username, passwordHash) values (0,?,?,?)"
        val preparedStmt: PreparedStatement = connection.prepareStatement(insertsql)
        preparedStmt.setString(1, f)
        preparedStmt.setString(2, u)
        preparedStmt.setString(3, p)
        preparedStmt.execute
        preparedStmt.close
        println("Do you want to be an admin?If so enter super duper secret password")
        val trynabeadmin = readLine
        admin = make_admin(trynabeadmin, u)
        print(admin)
      }
      /*val prpStmt2: PreparedStatement = connection.prepareStatement("UPDATE users " + s"SET administrator = 1 WHERE username = ?")
      prpStmt2.setString(1,u)
      prpStmt2.executeUpdate()
*/
      //val result = s"INSERT INTO users (administrator, firstName,username,passwordHash) VALUES (0,$f,$u,$p)"
      //val resultSet = statement.executeUpdate(result)


    } catch {
      case e: Throwable => e.printStackTrace
    }
    println("Do you want to login? Enter Yes/y or No/n")
    val user_login = readLine()
    if (user_login == "Yes" || user_login== "y") {
      println("Enter username information")
      val username_login_attempt = readLine()
      println("Enter password information")
      val password_login_attempt = readLine()
      val logged_in = login(username_login_attempt, password_login_attempt)

    }


    var n = 1

    /* --- ADMIN ACTIVE ---*/
    while (logged_in && admin && n != 0) {
      val admin_action = admin_menu()
      if (admin_action == 1) {
        println("You want to make a new admin? Be careful who you give the power to")
        println("Enter the username of the person you want to upgrade")
        val makin_admin = readLine()
        println("Enter the super secret password")
        val makin_admin_password = readLine()
        make_admin(makin_admin_password, makin_admin)
      } else if (admin_action == 2) {
        println("You want to make a query?\nHere are your tables\n1) Netflix\n2)Disney\n3)Hulu")
        println("What do you want to search?")
        val query_choice = readInt()

        println("You chose " + query_choice + ". Now showing possible queries")
        val using_db = convert_db(query_choice) // change the number to the database name

        println("1)Count how many TV Shows in " + using_db)
        println("2)Count how many Movies in " + using_db)
        println("3)Whats the top 3 genres in  " + using_db)
        println("4)How many have TV shows /Movies been released from a certain year " + using_db)
        println("5) How many have TV Show / Movies been released from a certain date")
        println("6) percentage given in a year")
        println("7) Projected percentage based on past three years")
        println("8) If you want to compare tables, go here!")

        println("Pick a number to perform the action.")

        val query_choice2 = readInt //user choses which query

        if (query_choice2 < 8) {
          admin_queries(query_choice2, using_db) // submit the query choice with the specified database
        }
        else{
          println("Here you get to compare databases")
          println("1) Which data base has more TV Shows?")
          println("2) Which database has more Movies?")
          println("3) Which database has more content?")
          println("4) Are there common TV/Movies between the databases?")
          var query_choice3 = readInt()
          println("What databases do you want to compare?\n" +
            "1)Netflix\n2)Disney\n3)Hulu")
          var db1 = readInt()
          var string_db1 = convert_db(db1)
          println(string_db1 + ". And?")
          var db2 = readInt()
          var string_db2 = convert_db(db2)
          admin_queries_compare(query_choice3,string_db1,string_db2)
        }

      } else if (admin_action == 3) {
        println("You want to update your password? Enter the username and the password.")
        println("Enter username")
        val new_username = readLine()
        println("Enter password")
        val new_password = readLine
        update_password(new_username,new_password)
      } else if(admin_action == 4) {
        println("Do you want to logout? Yes or y")
        val logging_out = readLine()
        if(logging_out == "y" || logging_out == "Yes") n = 0 else n = 1
      }else {
        println("Not a valid input")
      }
    }
    /*--- END OF ADMIN ---*/

    /*------ Start of base user ----- */
    while (logged_in && !admin && n !=0){
      println("Welcome user")
      println("Select an option\n" +
        "1) Search by title\n" +
        "2)Search by year\n" +
        "3)Search by director\n4)Find common shows\n5)Logout\n6)Change password")
      println("Enter your choice:")
      val user_choice = readInt()
      println("You chose"+ user_choice)


      if (user_choice == 1){
        println("Enter the database you want to search 1)Netflix\n2)Disney\n3)Hulu")
        val d = readInt()
        val db= convert_db(d)
        println("Enter the title")
        var title = readLine()
        search_database(db,title)
      }else if(user_choice== 2){
        println("Enter the database you want to search 1)Netflix\n2)Disney\n3)Hulu")
        val d = readInt()
        val db= convert_db(d)
        println("Enter the year")
        var year = readInt
        search_year(db,year)
      }else if(user_choice== 3){
        println("Enter the database you want to search 1)Netflix\n2)Disney\n3)Hulu")
        val d = readInt()
        val db= convert_db(d)
        println("Enter the director")
        var director = readLine()
        search_director(db,director)
      }else if(user_choice == 4){
        println("What two databases do you want to compare? Enter 1)Netflix\n2)Disney\n3)Hulu")
        val d1 = readInt()
        val db1= convert_db(d1)
        println(db1+ ". And?")
        val d2 = readInt()
        val db2= convert_db(d2)
        find_common(db1,db2)
      }
      else if(user_choice == 5){
        println("Do you want to logout? Yes or y")
        val logging_out = readLine()
        if(logging_out == "y" || logging_out == "Yes") n = 0 else n = 1
      }
      else if(user_choice == 6){
        println("Do you want to change password? Enter yes or no")
        val user_ans = readLine()
        if(user_ans == "yes" || user_ans == "y") {
          println("Enter username")
          val us = readLine
          println("Enter password")
          val pw = readLine
          update_password(us,pw)
        }
        else {println("Not today")}
      }
      else{println("Not a valid option")}
    }

   // spark.sql(s"select $net.title as `$net`, $d.title as `$net`, $net.date_added as `$net date`, $d.date_added as `$d date` from $net join $d on $net.title = $d.title").show()
    // set as view then count the rows
    //
    // spark.sql("select listed_in, max(listed_in) from maximum")
//    spark.sql("Drop view if exists covid_release")
//    spark.sql("Select title from netflix1 where release_year > 2019").show()
//    spark.sql("create view if not exists covid_release as (Select count(title) as `Count` from netflix1 where release_year > 2019 and type = 'Movie') ")
//    spark.sql("select * from covid_release").show()


//    spark.sql(sqlText="create table IF NOT EXISTS netflix_shows (show_id string, title String, director array<String>, cast array<string>, country array<String>, date_added date, release_year int, rating String, duration String, listed_in String, description Array<string>) partitioned by (type String) row format delimited fields terminated by ',' collection items terminated by '|' ")
//    spark.sql("LOAD DATA LOCAL INPATH 'input/p3.csv' INTO TABLE netflix_shows partition (type='Movie')")
//    spark.sql("LOAD DATA LOCAL INPATH 'input/p3.csv' INTO TABLE netflix_shows partition (type='TV Show')")
 //spark.sql("SELECT count(*) FROM netflix_shows where type ='TV Show'").show()
   // spark.sql("show partitions netflix_shows").show()

    // spark.sql("select * from netflix_shows partition (type=Tv Show)").show()

    // average amount
    //spark.sql("Select ( (Select count(*) from netflix1 where type='Movie' and release_year = 2018) + (Select count(*) from netflix1 where type='Movie' and release_year = 2019) + (select count(*) from netflix1 where type='Movie' and release_year = 2020) + (Select count(*) from netflix1 where type='Movie' and release_year = 2021)) / 4").show()

   // spark.sql("Select ( (Select count(*) from netflix1 where type='Movie' and release_year = 2019) + (select count(*) from netflix1 where type='Movie' and release_year = 2020) + (Select count(*) from netflix1 where type='Movie' and release_year = 2021)) / 3").show()
   // spark.sql("Select ((Select count(*) from netflix1 where type='Movie' and release_year = 2019) + (select count(*) from netflix1 where type='Movie' and release_year = 2020)) / 2").show()
  }

}
