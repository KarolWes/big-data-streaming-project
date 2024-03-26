package com.example.bigdata;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import java.text.SimpleDateFormat;

public class EsperClient {
    public static void main(String[] args) throws InterruptedException {
        int noOfRecordsPerSec;
        int howLongInSec;
        if (args.length < 2) {
            noOfRecordsPerSec = 10;
            howLongInSec = 61;
        } else {
            noOfRecordsPerSec = Integer.parseInt(args[0]);
            howLongInSec = Integer.parseInt(args[1]);
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("H:m");
        Configuration config = new Configuration();
        //config.getCommon().addEventType(JokeData.class);
        EPRuntime runtime = EPRuntimeProvider.getRuntime("http://localhost:port", config);


        EPDeployment deployment = compileAndDeploy(runtime, """
              @public @buseventtype create json schema JokeEvent(character string, quote string, people_in_room int, laughing_people int, pub string, ets string, its string);
               
               
              create window McL#length(100) as JokeEvent;
              insert into McL select * from JokeEvent;
              
              @name('answer') select m[0].laughing_people as lp1, m[1].laughing_people as lp2, m[2].laughing_people as lp3
              from pattern[ every ([3] m=JokeEvent(laughing_people >= 4) until JokeEvent(people_in_room >= 30))] ;
              """);

        SimpleListener listener = new SimpleListener();
        // Add a listener to the statement to handle incoming events
        for (EPStatement statement : deployment.getStatements()) {
            statement.addListener(listener);
        }

//        InputStreamGenerator generator = new InputStreamGenerator(noOfRecordsPerSec, howLongInSec);
//        generator.generate(runtime, true);
        for (String s : createInputData()) {
            runtime.getEventService().sendEventJson(s, "JokeEvent");
        }



    }

    public static EPDeployment compileAndDeploy(EPRuntime epRuntime, String epl) {
        EPDeploymentService deploymentService = epRuntime.getDeploymentService();
        EPDeployment deployment;

        CompilerArguments args =
                new CompilerArguments(epRuntime.getConfigurationDeepCopy());
        try {
            EPCompiled epCompiled = EPCompilerProvider.getCompiler().compile(epl, args);
            deployment = deploymentService.deploy(epCompiled);
        } catch (EPCompileException | EPDeployException e) {
            throw new RuntimeException(e);
        }
        return deployment;
    }

    private static EPCompiled getEPCompiled(Configuration config) {
        CompilerArguments compilerArgs = new CompilerArguments(config);

        // Compile the EPL statement
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPCompiled epCompiled;
        try {
            String epl_1 = "";

            epCompiled = compiler.compile(epl_1, compilerArgs);

        }
        catch (EPCompileException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }
        return epCompiled;
    }
    static String[] createInputData() {
        return new String[] {
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":0, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:00:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":1, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:01:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":3, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:02:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":3, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:03:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":10, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:04:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":5, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:05:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":5, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:06:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":1, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:07:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":1, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:08:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":4, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:09:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":1, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:10:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":7, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:11:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":1, \"pub\":\"Bob's\", \"ets\":0, \"its\":\"2011-04-01 00:12:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":0, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:13:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":5, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:14:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":3, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:15:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":4, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:16:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":32, \"laughing_people\":4, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:17:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":5, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:18:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":8, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:19:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":7, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:20:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":8, \"pub\":\"Bob's\", \"ets\":0, \"its\":\"2011-04-01 00:21:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":1, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:22:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":7, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:23:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":1, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:24:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":0, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:25:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":0, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:26:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":0, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:27:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":4, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:28:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":1, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:29:00.0\"}",
                "{\"character\":\"\", \"quote\":\"\", \"people_in_room\":0, \"laughing_people\":1, \"pub\":\"McLaren's\", \"ets\":0, \"its\":\"2011-04-01 00:30:00.0\"}"
        };
    }

}

