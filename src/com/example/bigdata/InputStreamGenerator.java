package com.example.bigdata;

import com.espertech.esper.runtime.client.EPRuntime;
import net.datafaker.Faker;
import net.datafaker.fileformats.Format;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class InputStreamGenerator {
    private int noOfRecordsPerSec;
    private int howLongInSec;

    public InputStreamGenerator(int noOfRecordsPerSec, int howLongInSec) {
        this.noOfRecordsPerSec = noOfRecordsPerSec;
        this.howLongInSec = howLongInSec;
    }

    public int getNoOfRecordsPerSec() {
        return noOfRecordsPerSec;
    }

    public void setNoOfRecordsPerSec(int noOfRecordsPerSec) {
        this.noOfRecordsPerSec = noOfRecordsPerSec;
    }

    public int getHowLongInSec() {
        return howLongInSec;
    }

    public void setHowLongInSec(int howLongInSec) {
        this.howLongInSec = howLongInSec;
    }

    public void generate(EPRuntime runtime) throws InterruptedException {
        Faker faker = new Faker(new Random(25));
        String record;

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + (1000L * howLongInSec)) {
            for (int i = 0; i < noOfRecordsPerSec; i++) {
                String character = faker.howIMetYourMother().character();
                String[] pubs = {"McLaren's Pub", "McGee's Pub", "Pemberton's Pub", "Flaming Saddles Saloon",
                        "As Is NYC", "Lilly's Craft", "Empanada Mama", "Southgate", "P&J Carney's Pub", "O'Donoghue's"};
                Random random = new Random();
                int index = random.nextInt(pubs.length);
                String selectedPub = pubs[index];
                Timestamp eTimestamp = faker.date().past(30, TimeUnit.SECONDS);
                eTimestamp.setNanos(0);
                Timestamp iTimestamp = Timestamp.valueOf(LocalDateTime.now().withNano(0));
                int people_in_room = faker.number().numberBetween(0,10);
                record = Format.toJson()
                        .set("character", () -> character)
                        .set("quote", () -> faker.howIMetYourMother().quote())
                        .set("people_in_room", () -> String.valueOf(people_in_room))
                        .set("laughing_people", () -> String.valueOf(faker.number().numberBetween(0,people_in_room)))
                        .set("pub", () -> selectedPub)
                        .set("ets", eTimestamp::toString)
                        .set("its", iTimestamp::toString)
                        .build().generate();
                runtime.getEventService().sendEventJson(record, "JokeEvent");
            }
            waitToEpoch();
        }
    }

    static void waitToEpoch() throws InterruptedException {
        long millis = System.currentTimeMillis();
        Instant instant = Instant.ofEpochMilli(millis) ;
        Instant instantTrunc = instant.truncatedTo( ChronoUnit.SECONDS ) ;
        long millis2 = instantTrunc.toEpochMilli() ;
        TimeUnit.MILLISECONDS.sleep(millis2+1000-millis);
    }
}
