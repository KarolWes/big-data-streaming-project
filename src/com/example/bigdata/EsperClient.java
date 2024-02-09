package com.example.bigdata;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import net.datafaker.Faker;
import net.datafaker.fileformats.Format;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class EsperClient {
    public static void main(String[] args) throws InterruptedException {
        int noOfRecordsPerSec;
        int howLongInSec;
        if (args.length < 2) {
            noOfRecordsPerSec = 20;//20
            howLongInSec = 30;
        } else {
            noOfRecordsPerSec = Integer.parseInt(args[0]);
            howLongInSec = Integer.parseInt(args[1]);
        }

        Configuration config = new Configuration();
        CompilerArguments compilerArgs = new CompilerArguments(config);

        // Compile the EPL statement
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPCompiled epCompiled;
        try {
//            epCompiled = compiler.compile("""
//                    @public @buseventtype create json schema ScoreEvent(house string, character string, score int, ts string);
//                    @name('result') SELECT * from ScoreEvent.win:time(10 sec)
//                    group by house
//                    having score > avg(score);""", compilerArgs);
            String epl_1 = """
                    @public @buseventtype create json schema JokeEvent(character string, quote string, people_in_room int, laughing_people int, ts string);
                    @name('result') SELECT character, avg(laughing_people), ts from JokeEvent.win:time(10 sec)
                    group by character;""";

            epCompiled = compiler.compile(epl_1, compilerArgs);

        }
        catch (EPCompileException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        // Connect to the EPRuntime server and deploy the statement
        EPRuntime runtime = EPRuntimeProvider.getRuntime("http://localhost:port", config);
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(epCompiled);
        }
        catch (EPDeployException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPStatement resultStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "result");

        // Add a listener to the statement to handle incoming events
        resultStatement.addListener( (newData, oldData, stmt, runTime) -> {
            for (EventBean eventBean : newData) {
                System.out.printf("R: %s%n", eventBean.getUnderlying());
            }
        });
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
                        .set("pubs", () -> selectedPub)
                        .set("ets", eTimestamp::toString)
                        .set("ts", () -> iTimestamp.toString())
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

