package com.margaret;

import java.sql.*;
import java.sql.Date;
import java.util.*;

public class Main {

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";        //Configure the driver needed
    static final String DB_CONNECTION_URL = "jdbc:mysql://localhost:3306/vet";     //Connection string – where's the database?
    static final String USER = "root";   //TODO replace with your username
    static final String PASSWORD = "itecitec";   //TODO replace with your password

    public static void main(String[] args) {

        Statement statement = null;
        Connection conn = null;
        ResultSet rsMin = null;

        PreparedStatement psInsert = null;
        LinkedList<Statement> allStatements = new LinkedList<Statement>();

        try {
            //Instantiate the driver
            Class.forName(JDBC_DRIVER);

        } catch (ClassNotFoundException cnfe) {
            System.out.println("Can't instantiate driver class; check you have drives and classpath configured correctly?");
            cnfe.printStackTrace();
            System.exit(-1);  //No driver? Need to fix before anything else will work. So quit the program
        }

        try{

            conn = DriverManager.getConnection(DB_CONNECTION_URL, USER, PASSWORD);
            statement = conn.createStatement();
            allStatements.add(statement);

            System.out.println("Average Weather Database Program");

            //Create a table in the database. Stores today's date, and the min and max temperatures recorded.

            String createTableSQL = "CREATE TABLE temp (day date, mintemp double, maxtemp double)";
            String deleteTableSQL = "DROP TABLE temp";
            try {
                statement.executeUpdate(createTableSQL);
                System.out.println("Created temp table");
            } catch (SQLException sqle) {
                //Seems the table already exists. Delete it and recreate it
                if (sqle.getSQLState().startsWith("42") ) {    //Error code for table already existing start with XO
                    System.out.println("Temp table appears to exist already, delete and recreate");
                    statement.executeUpdate(deleteTableSQL);
                    statement.executeUpdate(createTableSQL);
                } else {
                    //Something else went wrong. If we can't create the table, no point attempting
                    //to run the rest of the code. Throw the exception again to be handled at the end of the program.
                    System.out.println("Got stuck in catch else " + sqle.getSQLState() + " is sql state");
                    throw sqle;
                }
            }

            //Add some test data

            String prepStatInsert = "INSERT INTO temp VALUES ( ?, ?, ? )";

            psInsert = conn.prepareStatement(prepStatInsert);
            allStatements.add(psInsert);

            psInsert.setDate(1, Date.valueOf("2014-04-01"));
            psInsert.setDouble(2, 44.2);
            psInsert.setDouble(3, 58.7);
            psInsert.executeUpdate();

            psInsert.setDate(1, Date.valueOf("2014-04-02"));
            psInsert.setDouble(2, 34.6);
            psInsert.setDouble(3, 55.1);
            psInsert.executeUpdate();

            psInsert.setDate(1, Date.valueOf("2014-04-03"));
            psInsert.setDouble(2, 43.9);
            psInsert.setNull(3, Types.DOUBLE);  //Forgot to record the max temperature for this date so set it to null.
            psInsert.executeUpdate();

            psInsert.setDate(1, Date.valueOf("2014-04-04"));
            psInsert.setDouble(2, 43.8);
            psInsert.setDouble(3, 47.2);
            psInsert.executeUpdate();


            System.out.println("Added test data to database");

            //Let's calculate the average minimum and average maximum temperature for all the days.
            //Add up all the maximum temperatures and divide by number of days to get average max temperature.
            //Add up all the minimum temperatures and divide by number of days to get average min temperature.

            double averageMaxTemp = 0;
            double averageMinTemp = 0;

            String getAvgsSQL = "SELECT AVG(mintemp) AS rsMin, AVG(maxtemp) AS rsMax FROM temp";
            rsMin = statement.executeQuery(getAvgsSQL);
            while (rsMin.next() ){ // there is only one
                averageMinTemp = rsMin.getDouble("rsMin");
                System.out.println("Average min is " + averageMinTemp);
                averageMaxTemp = rsMin.getDouble("rsMax");
                System.out.println("Average max is " + averageMaxTemp);
            }

            System.out.println("Average maximum temperature = " + averageMaxTemp + " , average minimum temperature = " + averageMinTemp);


        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        finally {
            //A finally block runs whether an exception is thrown or not. Close resources and tidy up whether this code worked or not.
            try {
                if (rsMin != null) {
                    rsMin.close();  //Close result set
                    System.out.println("ResultSet closed");
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }

            //Close all of the statements. Stored a reference to each statement in allStatements so we can loop over all of them and close them all.
            for (Statement s : allStatements) {

                if (s != null) {
                    try {
                        s.close();
                        System.out.println("Statement closed");
                    } catch (SQLException se) {
                        System.out.println("Error closing statement");
                        se.printStackTrace();
                    }
                }
            }

            try {
                if (conn != null) {
                    conn.close();  //Close connection to database
                    System.out.println("Database connection closed");
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }

        System.out.println("End of program");
    }
}

