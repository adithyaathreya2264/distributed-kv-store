package com.dkv.client;

import java.util.Arrays;
import java.util.Scanner;

public class ClientMain {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: ClientMain <host:port>");
            System.exit(1);
        }

        String seed = args[0];
        System.out.println("Connecting to " + seed);

        DKVClient client = new DKVClient(Arrays.asList(seed));
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Interactive KV Client. Type 'help' for commands.");

            boolean running = true;
            while (running) {
                System.out.print("> ");
                if (!scanner.hasNextLine())
                    break;
                String line = scanner.nextLine();
                String[] parts = line.split("\\s+");
                if (parts.length == 0 || parts[0].isEmpty())
                    continue;
                String cmd = parts[0].toLowerCase();

                try {
                    switch (cmd) {
                        case "put":
                            if (parts.length < 3) {
                                System.out.println("Usage: put <key> <value>");
                            } else {
                                client.put(parts[1], parts[2]).get();
                                System.out.println("OK");
                            }
                            break;
                        case "get":
                            if (parts.length < 2) {
                                System.out.println("Usage: get <key>");
                            } else {
                                String val = client.get(parts[1]).get();
                                System.out.println("Value: " + val);
                            }
                            break;
                        case "delete":
                            if (parts.length < 2) {
                                System.out.println("Usage: delete <key>");
                            } else {
                                client.delete(parts[1]).get();
                                System.out.println("OK");
                            }
                            break;
                        case "exit":
                        case "quit":
                            running = false;
                            break;
                        case "help":
                            System.out.println("Commands: put <k> <v>, get <k>, delete <k>, exit");
                            break;
                        default:
                            System.out.println("Unknown command");
                    }
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                }
            }
        }
        client.close();
        System.exit(0);
    }
}
