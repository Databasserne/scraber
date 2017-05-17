package com.mycompany.citiesscraber;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
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
    private static final String MYSQL_PASS = "1234";

    private static final String NEO4J_USER = "neo4j";
    private static final String NEO4J_PASS = "1234";

    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException, SAXException, ParserConfigurationException {
        insertCities(getNeo4jCon(), getCities());
        //HashSet<City> cities = getCitiesFromDb();

        insertBooks(getNeo4jCon());

//        String bookPath = "books/ebooks/";
//        File dir = new File(bookPath);
//        File[] books = dir.listFiles();
//        for (File book : books) {
//            //File testFile = new File("1025.txt"); // For testing, running single book.
//            city = scrabeCity(cities, book);
//        }
    }

    private static HashSet<String> scrabeCity(HashSet<String> citiesToFind, File file, String title) throws FileNotFoundException, IOException, ParserConfigurationException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
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

            book.retainAll(citiesToFind);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            br.close();
        }
        return book;
    }

    private static void insertCitiesForBook(Driver driver, HashSet<String> cities, String bookTitle){
        /*
            Match(b:Book{name: 'Byron'})
            Match(c1:City{name: 'Winterberg'}),(c2:City{name: 'Edgemere'})
            Create (b)-[:Mentions]->(c1)
            Create (b)-[:Mentions]->(c2)
        */
        if(cities.size() <= 0)
            return;
        
        for (String city : cities) {
            String query = "Match(b:Book{name: \""+bookTitle+"\"})\n" +
                    "Match(c:City {name: '" + city + "'})" +
                    "Create (b)-[:Mentions]->(c)";
            try (Session session = driver.session()) {
                session.run(query);
            }
        }
    }
    
    private static void insertCitiesForBook(Connection con, HashSet<String> cities, String bookTitle) {
        try {
            stmt = con.prepareStatement("SELECT * FROM Books WHERE Name = ?");
            stmt.setString(1, bookTitle);
            result = stmt.executeQuery();
            Long bookId = 0L;
            if (result.next()) {
                bookId = result.getLong(1);

                for (String city : cities) {
                    stmt = con.prepareStatement("SELECT id FROM Cities WHERE Name = ?");
                    stmt.setString(1, city);
                    result = stmt.executeQuery();
                    Long cityId = 0L;
                    if (result.next()) {
                        cityId = result.getLong(1);

                        stmt = con.prepareStatement("INSERT INTO Books_Cities (Book_ID, City_ID) VALUES (?, ?)");
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
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("cities5000.txt"), "UTF-8"));
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

    private static void insertCities(Driver driver, HashSet<City> cities) {
        int c = 1;
        for (City city : cities) {
            StatementResult result;
            String cityName = StringEscapeUtils.escapeJava(city.getName());
            try (Session session = driver.session()) {
                session.run("merge (c:City {name: \"" + cityName + "\", "
                        + "Geolat: " + city.getGeolat() + ", Geolng: " + city.getGeolng() + "})");
            }
            System.out.println(c++ + "/" + cities.size());
        }
    }

    private static void insertCities(Connection con, HashSet<City> cities) {
        try {
            HashSet<String> tmpCities = new HashSet<>();
            stmt = con.prepareStatement("SELECT Name FROM Cities");
            result = stmt.executeQuery();
            while (result.next()) {
                tmpCities.add(result.getString(1));
            }
            System.out.println("Done with fetching cities");
            int count = 1;
            for (City city : cities) {
                if (!tmpCities.contains(city.getName())) {
                    stmt = con.prepareStatement("INSERT INTO Cities (Name, Geolat, Geolng) VALUES (?, ?, ?)");
                    stmt.setString(1, city.getName());
                    stmt.setFloat(2, city.getGeolat());
                    stmt.setFloat(3, city.getGeolng());

                    stmt.executeUpdate();

                }
                count++;
                if (count % 100 == 0) {
                    System.out.println("Inserted " + count + " out of " + cities.size() + " cities");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static HashSet<City> getCitiesFromDb(Connection con) {
        HashSet<City> cities = new HashSet<>();
        try {
            stmt = con.prepareStatement("SELECT * FROM Cities");
            result = stmt.executeQuery();
            while (result.next()) {
                City city = new City(result.getString("Name"), result.getFloat("Geolat"), result.getFloat("Geolng"));
                city.setId(result.getLong("id"));
                cities.add(city);
            }
        } catch (Exception e) {
        }

        return cities;
    }

    private static HashSet<String> getCitiesFromDb(Driver driver) {
        HashSet<String> cities = new HashSet<>();
        
        StatementResult result;
        try (Session session = driver.session()) {
            result = session.run("Match(c:City) "
                    + "return DISTINCT c.name as name ");
        }
        
        while (result.hasNext()) {
            Record record = result.next();
            cities.add(record.get("name").asString());
        }

        return cities;
    }
    
    private static HashSet<Book> getBooksFromDb() {
        HashSet<Book> books = new HashSet<>();
        try {
            stmt = getConnection().prepareStatement("SELECT * FROM Books");
            result = stmt.executeQuery();
            while (result.next()) {
                Book book = new Book(result.getString("Name"));
                book.setId(result.getLong("id"));
                books.add(book);
            }
        } catch (Exception e) {
        }

        return books;
    }

    private static void insertBooks(Driver driver) throws IOException, FileNotFoundException, ParserConfigurationException {
        String bookPath = "books/ebooks/";
        File dir = new File(bookPath);
        File[] books = dir.listFiles();
        int c = 1;
        HashSet<String> cities = getCitiesFromDb(driver);
        for (File book : books) {
            long time = System.currentTimeMillis();

            String bookNumber = book.getName().split("\\.")[0];
            MetaData metaData = getMetadataFromBook(bookNumber);
            if (metaData == null) {
                continue;
            }

            insertBook(driver, metaData.getName(), metaData.getAuthor());
            insertCitiesForBook(driver, scrabeCity(cities, book, metaData.getName()), metaData.getName());

            System.out.println("Time: " + (System.currentTimeMillis() - time));

            System.out.println("Book " + c++ + " out of " + books.length);
        }
    }

    private static void insertBooks(Connection con) throws IOException, ParserConfigurationException, ClassNotFoundException {
        String bookPath = "books/ebooks/";
        File dir = new File(bookPath);
        File[] books = dir.listFiles();
        int c = 1;
        HashSet<City> cities = getCitiesFromDb(con);
        for (File book : books) {
            long time = System.currentTimeMillis();

            String bookNumber = book.getName().split("\\.")[0];
            MetaData metaData = getMetadataFromBook(bookNumber);
            if (metaData == null) {
                continue;
            }

            insertBook(con, metaData.getName(), metaData.getAuthor());
            insertCitiesForBook(con, scrabeCity(cities, book, metaData.getName()), metaData.getName());

            System.out.println("Time: " + (System.currentTimeMillis() - time));

            System.out.println("Book " + c++ + " out of " + books.length);
        }
    }

    private static void insertBook(Driver driver, String title, String author) {
        String bookTitle = StringEscapeUtils.escapeJava(title);
        try (Session session = driver.session()) {
            session.run("create (b:Book{name: \""+ bookTitle + "\"})"
                    + "merge(a:Author{name: \""+author+"\"})"
                    + "create (a)-[r:Authored]->(b)");
        }
    }

    private static void insertBook(Connection con, String title, String author) {
        try {
            stmt = con.prepareStatement("INSERT INTO Books (Name) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            stmt.setString(1, title);
            stmt.executeUpdate();
            result = stmt.getGeneratedKeys();
            Long bookId = 0L;
            if (result.next()) {
                bookId = result.getLong(1);
            }

            stmt = con.prepareStatement("SELECT id FROM Authors WHERE Name = ?");
            stmt.setString(1, author);
            result = stmt.executeQuery();
            Long authorId = 0L;
            if (result.next()) {
                authorId = result.getLong(1);
            } else {
                stmt = con.prepareStatement("INSERT INTO Authors (Name) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
                stmt.setString(1, author);
                stmt.executeUpdate();
                result = stmt.getGeneratedKeys();
                if (result.next()) {
                    authorId = result.getLong(1);
                }
            }

            stmt = con.prepareStatement("INSERT INTO Books_Authors (Book_ID, Author_ID) VALUES (?, ?)");
            stmt.setLong(1, bookId);
            stmt.setLong(2, authorId);
            stmt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Driver getNeo4jCon() {
        return GraphDatabase.driver(
                "bolt://localhost:7687",
                AuthTokens.basic(NEO4J_USER, NEO4J_PASS));
    }

    private static MetaData getMetadataFromBook(String bookFileName) throws IOException, ParserConfigurationException {
        String ebookPath = "books/metadata/%s/pg%s.rdf";

        File meta = new File(String.format(ebookPath, bookFileName, bookFileName));
        if (!meta.exists()) {
            System.out.println("No meta data found!");
            System.out.println("");
            return null;
        }

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc;
        try {
            doc = dBuilder.parse(meta);
        } catch (SAXException e) {
            System.out.println("Cound not open meta data file");
            System.out.println("");
            return null;
        }
        doc.getDocumentElement().normalize();

        String title = doc.getElementsByTagName("dcterms:title").item(0).getTextContent();
        NodeList creator = doc.getElementsByTagName("pgterms:name");
        String author = "Unknown";
        if (creator.getLength() > 0) {
            author = creator.item(0).getTextContent();
        }

        MetaData metaData = new MetaData();
        metaData.setName(title);
        metaData.setAuthor(author);

        return metaData;
    }

}
