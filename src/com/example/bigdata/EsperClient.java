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
               
              @name('answer') select pub, its.substring(11, 16) as its_start,
              from JokeEvent#time_batch(1 min)
              group by its.substring(11, 16), pub
              having sum (case when laughing_people * 2 > people_in_room then 1 else (case when laughing_people * 2 = people_in_room then 0 else -1 end) end) < 0;
                 """);

        SimpleListener listener = new SimpleListener();
        // Add a listener to the statement to handle incoming events
        for (EPStatement statement : deployment.getStatements()) {
            statement.addListener(listener);
        }

        InputStreamGenerator generator = new InputStreamGenerator(noOfRecordsPerSec, howLongInSec);
        generator.generate(runtime, true);



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


}

