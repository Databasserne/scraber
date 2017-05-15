/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.citiesscraber;

/**
 *
 * @author Vixo
 */
public class City {
    private Long id;
    private String name;
    private float geolat;
    private float geolng;

    public City() {
    }
    
    public City(String name, float geolat, float geolng) {
        this.name = name;
        this.geolat = geolat;
        this.geolng = geolng;
    }
    
    public void setId(Long id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setGeolat(float geolat) {
        this.geolat = geolat;
    }

    public void setGeolng(float geolng) {
        this.geolng = geolng;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public float getGeolat() {
        return geolat;
    }

    public float getGeolng() {
        return geolng;
    }
}
