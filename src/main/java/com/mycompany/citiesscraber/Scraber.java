package com.mycompany.citiesscraber;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import static java.util.stream.Collectors.toList;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author alexander
 */
public class Scraber {

    private static PreparedStatement stmt;
    private static ResultSet result;
    
    private static final String DRIVER_MYSQL = "jdbc:mysql://localhost:3306/gutenberg?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASS = "wa";
    
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
        long time = System.currentTimeMillis();
        
        //insertCities(getConnection(), getCities());
        //HashSet<City> cities = getCitiesFromDb();
        
        String bookPath = "books/ebooks/";
        String ebookPath = "books/metadata/epub/%s/pg%s.rdf";
        File dir = new File(bookPath);
        File[] books = dir.listFiles();
        for(File book : books) {
            String bookNumber = book.getName().split("\\.")[0];
            File meta = new File(String.format(ebookPath, bookNumber, bookNumber));
            
            System.out.println(meta.getName());
            System.out.println(book.getName());
        }
        
        HashSet<String> city = null;
        //for (int i = 0; i < 10000; i++) {
//            city = scrabeCity(cities, "1025.txt");
  //      }
        //city = scrabeCity(cities, "1025.txt");
        System.out.println("Time: " + (System.currentTimeMillis() - time));
        
//        for (String string : city) {
//            System.out.println(string);
//        }
    }
    
    private static HashSet<String> scrabeCity(HashSet<City> citiesToFind, String textFile) throws FileNotFoundException, IOException {
        HashSet<String> found;
        
        BufferedReader br = new BufferedReader(new FileReader(textFile));
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            String everything = sb.toString();
            
            HashSet<String> book = new HashSet<>(Arrays.asList(everything.split(" ")));
            
            book.retainAll(citiesToFind);
            
            found = book;

        } finally {
            br.close();
        }
        
        return found;
    }

    private static HashSet<City> getCities() throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader("cities5000.txt"));
        HashSet<City> cities = new HashSet<>();
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            String everything = sb.toString();
            
            String[] list = everything.split("\n");
            for (String string : list) {
                String[] oneCity = string.split("\t");
                City city = new City(oneCity[1], Float.parseFloat(oneCity[4]), Float.parseFloat(oneCity[5]));
                cities.add(city);
            }
        } finally {
            br.close();
        }

        return cities;
    }
    
    private static Connection getConnection() throws ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        
        try {
            return DriverManager.getConnection(DRIVER_MYSQL, MYSQL_USER, MYSQL_PASS);
        } catch (Exception e) {
        }
        return null;
    }
    
    private static void insertCities(Connection con, HashSet<City> cities) {
        try {
            for(City city : cities) {
                // Check if city exists.
                stmt = con.prepareStatement("SELECT * FROM cities WHERE Name = ?");
                stmt.setString(1, city.getName());
                result = stmt.executeQuery();
                
                if(!result.next()) {
                    stmt = con.prepareStatement("INSERT INTO cities (Name, Geolat, Geolng) VALUES (?, ?, ?)");
                    stmt.setString(1, city.getName());
                    stmt.setFloat(2, city.getGeolat());
                    stmt.setFloat(3, city.getGeolng());
                    
                    stmt.executeUpdate();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static HashSet<City> getCitiesFromDb() {
        HashSet<City> cities = new HashSet<>();
        try {
            stmt = getConnection().prepareStatement("SELECT * FROM cities");
            result = stmt.executeQuery();
            while(result.next()) {
                City city = new City(result.getString("Name"), result.getFloat("Geolat"), result.getFloat("Geolng"));
                city.setId(result.getLong("id"));
                cities.add(city);
            }
        } catch (Exception e) {
        }
        
        return cities;
    }
    
    private static void insertBooks(Connection con, HashSet<String> books) {
        
    }
}
