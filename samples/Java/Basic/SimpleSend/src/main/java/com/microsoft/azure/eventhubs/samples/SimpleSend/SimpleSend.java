/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.samples.SimpleSend;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class SimpleSend {

    public static void main(String[] args)
            throws EventHubException, ExecutionException, InterruptedException, IOException {

        final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
                .setNamespaceName("bodends1-eventhub") // to target National clouds - use .setEndpoint(URI)
                .setEventHubName("import")
                .setSasKeyName("RootManageSharedAccessKey")
                .setSasKey("enmkJX9X2mA83VvSnsqDI8b52rnyPsvXNWyxHVzQO0s=");

        final Gson gson = new GsonBuilder().create();
//        Endpoint=sb://bodends1-eventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=enmkJX9X2mA83VvSnsqDI8b52rnyPsvXNWyxHVzQO0s=

        // The Executor handles all asynchronous tasks and this is passed to the EventHubClient instance.
        // This enables the user to segregate their thread pool based on the work load.
        // This pool can then be shared across multiple EventHubClient instances.
        // The following sample uses a single thread executor, as there is only one EventHubClient instance,
        // handling different flavors of ingestion to Event Hubs here.
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

        // Each EventHubClient instance spins up a new TCP/SSL connection, which is expensive.
        // It is always a best practice to reuse these instances. The following sample shows this.
        final EventHubClient ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString(), executorService);

        String pathName = "/home/chaitanya/Documents/Boden_Eventhub_Load/Eventhub_25K";
        final File folder = new File(pathName);
        FileFilter filter = new FileFilter() {

            public boolean accept(File f)
            {
                return f.getName().endsWith("json");
            }
        };

        long totalRecords = Objects.requireNonNull(folder.listFiles(filter)).length;
        System.out.println(DateTime.now().toString() + ": Found " + totalRecords + " in this folder "+ folder.getAbsolutePath());
        System.out.println();
        long batchSizeToSleep = 1000;
        short timeToSleepInSeconds = 120;
        long loopCounter = 0;
        long errorCounter = 0;

        File logFile = new File(pathName + File.separator+ "logs" + File.separator + "log.txt");
        if (!logFile.exists()) {
            logFile.createNewFile();
        }

        try {
            for (final File fileEntry : Objects.requireNonNull(folder.listFiles(filter))) {
                if (fileEntry.isFile()) {
                    loopCounter++;
//                    System.out.println(fileEntry.getName());
                    // check if data is valid json
                    byte[] payloadBytes = FileUtils.readFileToByteArray(fileEntry);
                    EventData sendEvent = EventData.create(payloadBytes);
                    ehClient.sendSync(sendEvent);
                    if(loopCounter % 250 == 0)
                        System.out.println(loopCounter);

                    if(loopCounter % batchSizeToSleep == 0 && loopCounter < totalRecords)
                    {
                        System.out.println("Starting sleep at" + DateTime.now().toString());
                        Thread.sleep(timeToSleepInSeconds*1000);
                    }
                }
                else
                {
                    BufferedWriter writer = new BufferedWriter(new FileWriter(logFile.getPath()));
                    writer.append(fileEntry.getName());
                    writer.close();
                    errorCounter++;
                }
            }
            System.out.println(loopCounter);
            System.out.println(errorCounter);
        }
        catch(Exception ex)
        {
            BufferedWriter writer = new BufferedWriter(new FileWriter(logFile.getPath()));
            writer.append(ex.toString());
            writer.close();
        }

        finally {
            ehClient.closeSync();
            executorService.shutdown();



        }

    /*
        try {
            for (int i = 0; i < 100; i++) {

                String payload = "Message " + Integer.toString(i);
                //PayloadEvent payload = new PayloadEvent(i);
                byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());
                EventData sendEvent = EventData.create(payloadBytes);

                // Send - not tied to any partition
                // Event Hubs service will round-robin the events across all Event Hubs partitions.
                // This is the recommended & most reliable way to send to Event Hubs.
                ehClient.sendSync(sendEvent);
            }

            System.out.println(Instant.now() + ": Send Complete...");
            System.out.println("Press Enter to stop.");
            System.in.read();
        } finally {
            ehClient.closeSync();
            executorService.shutdown();
        }

    */

    }
}
