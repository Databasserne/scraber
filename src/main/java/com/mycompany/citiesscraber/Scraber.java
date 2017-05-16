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
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

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
    private static final String MYSQL_PASS = "mk101593";
    
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException, SAXException, ParserConfigurationException {
        long time = System.currentTimeMillis();
        
        //insertCities(getConnection(), getCities());
        HashSet<City> cities = getCitiesFromDb();
        //HashSet<Book> books = getBooksFromDb();
        
        //insertBooks(getConnection());
        
        
        HashSet<String> city = null;
        
        String bookPath = "books/ebooks/";
        File dir = new File(bookPath);
        File[] books = dir.listFiles();
        //for (File book : books) {
            File testFile = new File("1025.txt"); // For testing, running single book.
            city = scrabeCity(cities, testFile);
        //}
        System.out.println("Time: " + (System.currentTimeMillis() - time));
        
//        for (String string : city) {
//            System.out.println(string);
//        }
    }
    
    private static HashSet<String> scrabeCity(HashSet<City> citiesToFind, File file) throws FileNotFoundException, IOException, ParserConfigurationException {
        String ebookPath = "books/metadata/epub/%s/pg%s.rdf";
        String bookNumber = file.getName().split("\\.")[0];
        File meta = new File(String.format(ebookPath, bookNumber, bookNumber));
        if(!meta.exists()){
            System.out.println("No meta data found!");
            System.out.println("");
            return null;
        }

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc;
        try {
            doc = dBuilder.parse(meta);
        }catch(SAXException e){
            System.out.println("Cound not open meta data file");
            System.out.println("");
            return null;
        }
        doc.getDocumentElement().normalize();
        String title = doc.getElementsByTagName("dcterms:title").item(0).getTextContent();
        
        HashSet<String> found = new HashSet<>();
        for(City city : citiesToFind) {
            found.add(city.getName());
        }
        
        BufferedReader br = new BufferedReader(new FileReader(file));
        HashSet<String> book = null;
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            String everything = sb.toString();

            book = new HashSet<>(Arrays.asList(everything.split(" ")));

            book.retainAll(found);
            
            insertCitiesForBook(getConnection(), book, title);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            br.close();
        }
        return book;
    }
    
    private static void insertCitiesForBook(Connection con, HashSet<String> cities, String bookTitle) {
        try {
            stmt = con.prepareStatement("SELECT * FROM books WHERE Name = ?");
            stmt.setString(1, bookTitle);
            result = stmt.executeQuery();
            Long bookId = 0L;
            if(result.next()) {
                bookId = result.getLong(1);
            
                for(String city : cities) {
                    stmt = con.prepareStatement("SELECT id FROM cities WHERE Name = ?");
                    stmt.setString(1, city);
                    result = stmt.executeQuery();
                    Long cityId = 0L;
                    if(result.next()) {
                        cityId = result.getLong(1);
                        
                        stmt = con.prepareStatement("INSERT INTO books_cities (Book_ID, City_ID) VALUES (?, ?)");
                        stmt.setLong(1, bookId);
                        stmt.setLong(2, cityId);
                        stmt.executeUpdate();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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
    
    private static HashSet<Book> getBooksFromDb() {
        HashSet<Book> books = new HashSet<>();
        try {
            stmt = getConnection().prepareStatement("SELECT * FROM books");
            result = stmt.executeQuery();
            while(result.next()) {
                Book book = new Book(result.getString("Name"));
                book.setId(result.getLong("id"));
                books.add(book);
            }
        } catch (Exception e) {
        }
        
        return books;
    }
    
    private static void insertBooks(Connection con) throws IOException, ParserConfigurationException {
        String bookPath = "books/ebooks/";
        String ebookPath = "books/metadata/epub/%s/pg%s.rdf";
        File dir = new File(bookPath);
        File[] books = dir.listFiles();
        int c = 0;
        for(File book : books) {
            String bookNumber = book.getName().split("\\.")[0];
            File meta = new File(String.format(ebookPath, bookNumber, bookNumber));
            if(!meta.exists()){
                System.out.println("No meta data found!");
                System.out.println("");
                continue;
            }
            
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc;
            try {
                doc = dBuilder.parse(meta);
            }catch(SAXException e){
                System.out.println("Cound not open meta data file");
                System.out.println("");
                continue;
            }
            doc.getDocumentElement().normalize();
            
            String title = doc.getElementsByTagName("dcterms:title").item(0).getTextContent();
            NodeList creator = doc.getElementsByTagName("pgterms:name");
            String author = "Unknown";
            if(creator.getLength() > 0)
                author = creator.item(0).getTextContent();
            
            System.out.println("Title : " + title);            
            System.out.println("Author : " + author);
            System.out.println("");
            
            insertBook(con, title, author);
        }
    }
    
    private static void insertBook(Connection con, String title, String author) {
        try {
            stmt = con.prepareStatement("INSERT INTO books (Name) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            stmt.setString(1, title);
            stmt.executeUpdate();
            result = stmt.getGeneratedKeys();
            Long bookId = 0L;
            if(result.next()) {
                bookId = result.getLong(1);
            }
            
            stmt = con.prepareStatement("SELECT id FROM authors WHERE Name = ?");
            stmt.setString(1, author);
            result = stmt.executeQuery();
            Long authorId = 0L;
            if(result.next()) {
                authorId = result.getLong(1);
            } else {
                stmt = con.prepareStatement("INSERT INTO authors (Name) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
                stmt.setString(1, author);
                stmt.executeUpdate();
                result = stmt.getGeneratedKeys();
                if(result.next()) {
                    authorId = result.getLong(1);
                }
            }
            
            stmt = con.prepareStatement("INSERT INTO books_authors (Book_ID, Author_ID) VALUES (?, ?)");
            stmt.setLong(1, bookId);
            stmt.setLong(2, authorId);
            stmt.executeUpdate();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
