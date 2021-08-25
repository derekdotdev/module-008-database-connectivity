import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/*

Author: Derek DiLeo
Course: COP3330C 
Date: 07/11/2021

A relational SQLite database with a simple Person class with the following attributes:

firstName (String)
lastName(String)
age (int)
ssn (long)
creditCard (long)

Project Requirements:

1. Demonstrate the insertion of a record into the database. Insert several records.

2. Write a method called insertPerson(Person person) that adds a person object to your database. 
	Create another object of type Person, and demonstrate calling your method, passing the object to the method. 
 
3. Demonstrate the retrieval of information from the database. 
	Use SQL Select statements, to retrieve a particular Person from the database.
 
4. Write a method called selectPerson that returns a Person object. 
	This method retrieves the data for a Person from the database. 
	We also need to pass a parameter to identify what person. 
	You can use ‘name’ if you like, or if you find it easier to use the database generated ID that’s fine too.  
	This method returns the object that represents that person.
	This will require that you extract the data that is returned from the database, and call the Person constructor.
	(Later you will understand that that this is the data-exchange between the relational database and the business layer. )

5. Write a method called findAllPeople that returns an ArrayList<Person> of objects containing all the people in the database.
Demonstrate that it is working correctly.

6. Write a method called deletePerson that removes a person from the database. 
	The parameters will be first name and last name.  
		Print out on the console the data from the record that is being deleted.
			Use your findAllPeople method to verify that that person has been removed from the database.
				Consider what this method should return. 
					Suppose the person is not found, should the method return that information somehow?
					*
					*/

public class DatabaseRunner {

	// Init DB directory and Connection(s)
	static String dir = "jdbc:sqlite:/Users/derekdileo/Documents/Software Development/Workspaces/javadatabase_programs/sqlite/db/";
	static String url = dir + "people.db";
	private static Connection c = null;

	// Connect to DB and notify current method being called (str)
	public static Connection connect(String str) {
		try {
			if (c == null) {
				c = DriverManager.getConnection(url);
			} else {
				c.close();
				c = DriverManager.getConnection(url);
			}
		} catch (SQLException e) {
			System.out.println("ERROR: SQL Exception!" + e.getMessage());
		}
		// Point of reference useful for debugging
		System.out.println("\nConnection to SQLite has been established via " + str + ".");
		return c;
	}
	
	// Create a relational database in existing folder path (url)
	public static void createNewDataBase(String fileName) {

		// Location of database to be created
		// Note: no Macintosh HD or escape characters in spaces
		String url = dir + fileName;

		// (try to) make a connection based on url
		try (Connection conn = DriverManager.getConnection(url)) {

			if (conn != null) {
				// Get metaData for connection to make sure it's got correct driver
				// Create database
				DatabaseMetaData meta = conn.getMetaData();
				// Print driver name to console
				System.out.println("The driver name is: " + meta.getDriverName());
				System.out.println("\nA new database has been created.");
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	// Method to connect to an existing database
	public static void createTable() {
		Connection conn = null;
		try {
			// Location reference (for debugging)
			System.out.println("Initial connection to SQLite has been established.");

			// Connect to the DB & notify
			conn = connect("createTable()");

			// Store string value to create a new table
			String sql = "CREATE TABLE IF NOT EXISTS people (\n" + " id integer PRIMARY KEY, \n"
					+ " firstName text NOT NULL, lastName text NOT NULL, age int NOT NULL, ssn long, creditCard long, \n"
					+ "  real\n" + ");";

			// Create, execute and close Statment to create table
			Statement stmt = conn.createStatement();
			stmt.execute(sql);
			stmt.close();

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				if (conn != null) {
					conn.close();
					System.out.println("\nThe createTable() method has finished!");
				}
			} catch (SQLException ex) {
				System.out.println(ex.getMessage());
			}
		}
	}

	// Initial retrieval Method (adapted from Java DB tutorial)
	private static void retrieveInfo() throws SQLException {

		// Connect to DB & notify while creating Statement (optional, less verbose)
		// Connection conn = connectTo("retrieveInfo()");
		// Using this format on remaining Methods to clean up code

		Statement stmt = connect("retrieveInfo()").createStatement();

		String sqlGetInfo = "SELECT id, firstName, lastName, age, ssn, creditCard FROM people";

		ResultSet rs = stmt.executeQuery(sqlGetInfo);

		// Loop through the ResultSet to print contents
		while (rs.next()) {
			System.out.println(rs.getInt("id") + "\t" + rs.getString("firstName") + "\t" + rs.getString("lastName")
					+ "\t" + rs.getInt("age") + "\t" + rs.getLong("ssn") + "\t" + rs.getLong("creditCard"));
		}
		rs.close();
		stmt.close();
	}

	// Extracted Method for manually adding "person" data to DB (not Person objects)
	private static void insertPeopleManual(String first, String last, int age, long ssn, long cc) throws SQLException {

		// Connect to DB & notify
		Connection conn = connect("insertPeopleManual()");

		// Create PreparedStatement with wildcards for Insertion
		String sqlInsert = "INSERT INTO people(firstName, lastName, age, ssn, creditCard) VALUES(?,?,?,?,?)";
		PreparedStatement pstmt = conn.prepareStatement(sqlInsert);

		// Wildcard entry
		pstmt.setString(1, first);
		pstmt.setString(2, last);
		pstmt.setInt(3, age);
		pstmt.setLong(4, ssn);
		pstmt.setLong(5, cc);
		pstmt.executeUpdate();
		pstmt.close();
		conn.close();
	}

	// Constructed Method for manually adding Person objects to DB
	public static void insertPerson(Person p) throws SQLException {

		// Connect to DB & notify
		// Connection conn = connect("insertPerson()"); <combined with pstmt below

		// Create PreparedStatement with wildcards for Insertion
		String sqlInsert = "INSERT INTO people(firstName, lastName, age, ssn, creditCard) VALUES(?,?,?,?,?)";
		PreparedStatement pstmt = connect("insertPerson()").prepareStatement(sqlInsert);

		pstmt.setString(1, p.getFirstName());
		pstmt.setString(2, p.getLastName());
		pstmt.setInt(3, p.getAge());
		pstmt.setLong(4, p.getSsn());
		pstmt.setLong(5, p.getCreditCard());
		pstmt.executeUpdate();

		pstmt.close();
		// conn.close(); < left for future reference
	}
	
	// A Method that returns a Person object based on ID parameter.
	public static Person selectPersonById(int i) throws SQLException {

		// Connect to DB & notify
		Statement stmt = connect("selectPersonById()").createStatement();

		String sqlGetInfo = "SELECT firstName, lastName, age, ssn, creditCard FROM people WHERE id=" + i;

		ResultSet rs = stmt.executeQuery(sqlGetInfo);

		// Create Person object based on ResultSet
		Person person = new Person(rs.getString("firstName"), rs.getString("lastName"), rs.getInt("age"),
				rs.getLong("ssn"), rs.getLong("creditCard"));

		rs.close();
		stmt.close();
		return person;
	}

	// Method that returns ArrayList<Person> all the people in the database.
	public static ArrayList<Person> findAllPeople() throws Exception {

		// Create ArrayList to hold people
		ArrayList<Person> people = new ArrayList<>();

		// Connect to DB, notify & create Statement
		Statement stmt = connect("findAllPeople()").createStatement();

		String sqlGetInfo = "SELECT id, firstName, lastName, age, ssn, creditCard FROM people";

		ResultSet rs = stmt.executeQuery(sqlGetInfo);

		while (rs.next()) {
			Person person = new Person(rs.getString("firstName"), rs.getString("lastName"), rs.getInt("age"),
					rs.getLong("ssn"), rs.getLong("creditCard"));
			people.add(person);
		}

		for (Person person : people) {
			System.out.println(person);
		}

		rs.close();
		stmt.close();

		return people;
	}
	
	// Method for removing a Person based on first & last name parameters
	// Part 1: collect and return ArrayList of ids to be deleted
	public static ArrayList<Integer> locateByNames(String firstName, String lastName) throws SQLException {

		// Create ArrayList for ids of those SELECTed
		ArrayList<Integer> ids = new ArrayList<>();

		// Connect to DB & notify
		Connection conn = connect("locateByNames()");

		try {
			String sqlGetInfo = "SELECT id, firstName, lastName, age, ssn, creditCard FROM people";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlGetInfo);
			while (rs.next()) {
				if (firstName.equals(rs.getString("firstName")) && lastName.equals(rs.getString("lastName"))) {
					// System.out.println("id to be deleted" + rs.getInt("id")); > Helpful for
					// debugging
					ids.add(rs.getInt("id"));
					}
			}
			stmt.close();
			rs.close();
		} catch (SQLException e) {
			System.out.println("locateByNames() catch: " + e.getMessage());
		}
		// Print out id list for reference (helpful for debugging)
		// System.out.println("Ids in ids list: " + ids);
		conn.close();
		return ids;
	}

	// Method for removing a Person based on first & last name parameters
	// Part 2: for each id in ids, delete the row
	public static void deletePerson(ArrayList<Integer> ids) throws Exception {
		
		System.out.println("\nArrayList<Integer> ids inside deletePerson() Method: " + ids);

		for (Integer id : ids) {
			Connection conn = null;
			try {
				// Helful for debugging
				// System.out.println("IDs made inside deletePerson() try block for loop: " +
				// id);

				conn = connect("deletePerson()");
				Statement stmt1 = conn.createStatement();

				// For INSERT, UPDATE, or DELETE use executeUpdate()
				// For SELECT, use executeQuery();
				String sqlDelete = "DELETE FROM people WHERE id = " + id;
				stmt1.executeUpdate(sqlDelete);
				stmt1.close();

			} catch (SQLException e) {
				System.out.println("deletePerson catch (trying to delete): " + e.getMessage());
			} finally {
				if (conn != null) {
					try {
						conn.close();
					} catch (SQLException e) {
						System.out.println("deletePerson finally catch (last): " + e.getMessage());
					}
				}
			}
		}
	}
	
	// Main method
	public static void main(String[] args) throws Exception {

		// Pass fileName String to create DB
		createNewDataBase("people.db");
		
		// Thinking
		System.out.println("------------------");
		
		// Create table in DB
		createTable();

		// Manually enter people to table
		System.out.println("\nManually Inserting Derek, Chris & Steve");
		insertPeopleManual("Derek", "DiLeo", 34, 987654, 123456);
		insertPeopleManual("Chris", "Lyon", 31, 122233, 145226);
		insertPeopleManual("Steve", "Lyon", 59, 544232, 844231);

		// Create Person objects to enter into table
		Person p1 = new Person("John", "Waters", 26, 1234, 5678);
		Person p2 = new Person("Joey", "DiLeo", 29, 1234, 5678);
		Person p3 = new Person("Jen", "LaPorte", 36, 1234, 5678);
		Person p4 = new Person("Fran", "Topping", 56, 1234, 5678);
		Person p5 = new Person("Mel", "Reis", 32, 1234, 5678);

		// Enter Person objects into table
		System.out.println("\nUsing insertPerson() to insert John, Joey, Jen, Fran & Mel");
		insertPerson(p1);
		insertPerson(p2);
		insertPerson(p3);
		insertPerson(p4);
		insertPerson(p5);

		// Retrieve all information from DB
		System.out.println("\nPrinting info via retrieveInfo() Method.");
		retrieveInfo();

		// Retrieve Person Object from DB by ID number & print
		Object obj = selectPersonById(5);
		System.out.println("\n");
		System.out.println(obj);

		// Retrieve all People to Demonstrate it's working
		System.out.println("\nPrinting from findAllPeople():");
		try {
			findAllPeople();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Delete all 'John Waters' entries
		try {
			ArrayList<Integer> ids = locateByNames("John", "Waters");
			deletePerson(ids);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Retrieve all People to see if Chris entries have been removed
		System.out.println("\nPrinting from findAllPeople() again (after John is removed):");
		try {
			findAllPeople();
		} catch (SQLException e) {
			// TODO Auto-generated
			e.printStackTrace();
		}
	}
}
