import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/*
Author: Derek DiLeo
Course: COP3330C 
Date: 07/11/2021

Peek at the relational SQLite database with a simple Person class
*/

public class DatabaseRunnerBasic {

	// Init DB directory and Connection(s)
	static String url = "jdbc:sqlite:/Users/derekdileo/Documents/Software Development/Workspaces/javadatabase_programs/sqlite/db/";
	static String urldb = url + "people.db";
	private static Connection c = null;

	// Connect to DB and notify current method being called (str)
	public static Connection connect(String str) {
		try {
			if (c == null) {
				c = DriverManager.getConnection(urldb);
			} else {
				c.close();
				c = DriverManager.getConnection(urldb);
			}
		} catch (SQLException e) {
			System.out.println("ERROR: SQL Exception!" + e.getMessage());
		}
		// Point of reference useful for debugging
		System.out.println("\nConnection to SQLite has been established via " + str + ".");
		return c;
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
	
	// Main method
	public static void main(String[] args) throws Exception {

		// Retrieve all People
		try {
			findAllPeople();
		} catch (SQLException e) {
			// TODO Auto-generated
			e.printStackTrace();
		}
	}
}
