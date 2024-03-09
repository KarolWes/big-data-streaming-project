package com.example.bigdata;

import java.util.Date;
import java.text.SimpleDateFormat;

public class JokeData {
    /*
     * `character` - nazwa postaci opowiadającej żart
     * `quote` - cytat
     * `people_in_room` - liczba osób w pokoju w chwili opowiedzenia żartu
     * `laughing_people` - liczba osób, których rozbawił żart
     * `pub` - pub, w którym żart został wypowiedziany
     * `ets` - czas opowiedzenia żartu w rzeczywistości
     * `its` - czas rejestracji w systemie
     */

    private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd, HH:mm:ss");

    private String character;
    private String quote;
    private int pplInRoom;
    private int laughing;
    private String pub;
    private Date ets;
    private Date its;

    public JokeData(String character, String quote, int pplInRoom, int laughing, String pub, Date ets, Date its) {
        this.character = character;
        this.quote = quote;
        this.pplInRoom = pplInRoom;
        this.laughing = laughing;
        this.pub = pub;
        this.ets = ets;
        this.its = its;
    }

    public String getCharacter() {
        return character;
    }

    public void setCharacter(String character) {
        this.character = character;
    }

    public String getQuote() {
        return quote;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }

    public int getPplInRoom() {
        return pplInRoom;
    }

    public void setPplInRoom(int pplInRoom) {
        this.pplInRoom = pplInRoom;
    }

    public int getLaughing() {
        return laughing;
    }

    public void setLaughing(int laughing) {
        this.laughing = laughing;
    }

    public String getPub() {
        return pub;
    }

    public void setPub(String pub) {
        this.pub = pub;
    }

    public Date getEts() {
        return ets;
    }

    public void setEts(Date ets) {
        this.ets = ets;
    }

    public Date getIts() {
        return its;
    }

    public void setIts(Date its) {
        this.its = its;
    }

    @Override
    public String toString() {
        return "JokeData{" +
                "character='" + character + '\'' +
                ", quote='" + quote + '\'' +
                ", pplInRoom=" + pplInRoom +
                ", laughing=" + laughing +
                ", pub='" + pub + '\'' +
                ", ets=" + df.format(ets) +
                ", its=" + df.format(its) +
                '}';
    }
}
