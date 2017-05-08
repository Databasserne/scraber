package com.mycompany.citiesscraber;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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

    public static void main(String[] args) throws IOException {
        HashSet<String> cities = getCities();
        HashSet<String> city = scrabeCity(cities, "1025.txt");
        for (String string : city) {
            System.out.println(string);
        }
    }
    
    private static HashSet<String> scrabeCity(HashSet<String> citiesToFind, String textFile) throws FileNotFoundException, IOException {
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

    private static HashSet<String> getCities() throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader("cities5000.txt"));
        HashSet<String> cities = new HashSet<>();
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
                cities.add(oneCity[1]);
            }
        } finally {
            br.close();
        }

        return cities;
    }
}
