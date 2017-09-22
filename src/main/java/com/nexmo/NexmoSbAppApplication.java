package com.nexmo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class NexmoSbAppApplication {

    public static Map<String, String> cliArgs;

    public static void main(String[] args) {
        Map<String, String> params = new HashMap<String, String>();

        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) {
                System.out.println("Argument must be '--file=filepath' or '--dir=dirpath' but got " + Arrays.toString(args));
                System.exit(1);
            }

            String rawKey = args[i].split("=")[0];
            String key = rawKey.substring(2, rawKey.length());
            String value = args[i].split("=")[1];
            params.put(key, value);
        }

        if ((params.containsKey("file") && params.containsKey("dir"))) {
            System.out.println("Argument must be '--file=filepath' or '--dir=dirpath', but not both");
            System.exit(1);
        } else if (!params.containsKey("file") && !params.containsKey("dir")) {
            System.out.println("Argument must be '--file=filepath' or '--dir=dirpath' but got " + Arrays.toString(args));
            System.exit(1);
        }

        System.out.println("Param check OK - launching with " + params.toString());
        cliArgs = params;

        SpringApplication.run(NexmoSbAppApplication.class, args);
    }
}
