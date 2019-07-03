/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.cosmosdb.sample;

import com.azure.data.cosmos.ConnectionPolicy;
import com.azure.data.cosmos.ConsistencyLevel;
import com.azure.data.cosmos.CosmosClient;
import com.azure.data.cosmos.CosmosClientException;
import com.azure.data.cosmos.CosmosContainer;
import com.azure.data.cosmos.CosmosContainerProperties;
import com.azure.data.cosmos.CosmosDatabase;
import com.azure.data.cosmos.CosmosDatabaseProperties;
import com.azure.data.cosmos.CosmosDatabaseResponse;
import com.azure.data.cosmos.CosmosItemProperties;
import com.azure.data.cosmos.CosmosItemResponse;
import com.azure.data.cosmos.FeedOptions;
import com.azure.data.cosmos.FeedResponse;
import com.azure.data.cosmos.PartitionKeyDefinition;
import com.azure.data.cosmos.SqlParameter;
import com.azure.data.cosmos.SqlParameterList;
import com.azure.data.cosmos.SqlQuerySpec;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {
    private final Scheduler scheduler;

    private CosmosClient client;
    private CosmosDatabase database;
    private CosmosContainer container;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    public Main() {
        // The SDK uses netty library for doing async IO operations. The IO operations are performed on the netty io threads.
        // The number of IO netty threads are limited; it is the same as the number of CPU cores.

        // The app should avoid doing anything which takes a lot of time from IO netty thread.
        // If the app consumes too much of IO netty thread you may face:
        //  * low throughput
        //  * bad latency
        //  * ReadTimeoutException because there is no netty IO thread available to read data from network.
        //  * deadlock

        // The app code will receive the data from Azure Cosmos DB on the netty IO thread.
        // The app should ensure the user's computationally/IO heavy work after receiving data
        // from Azure Cosmos DB is performed on a custom thread managed by the user (not on the SDK netty IO thread).
        //
        // If you are doing heavy work after receiving the result from the SDK,
        // you should provide your own scheduler to switch thread.

        // the following scheduler is used for switching from netty thread to user app thread.
        scheduler = Schedulers.elastic();
    }

    public void close() {
        client.close();
    }

    /**
     * Run a Hello Azure Cosmos DB console application.
     *
     * @param args command line args.
     */
    public static void main(String[] args) {
        Main p = new Main();

        try {
            p.getStartedDemo();
            System.out.println(String.format("Demo complete, please hold while resources are released"));
        } catch (Exception e) {
            System.err.println(String.format("Cosmos DB GetStarted failed with %s", e));
        } finally {
            System.out.println("close the client");
            p.close();
        }
        System.exit(0);
    }

    private void getStartedDemo() throws Exception {
        System.out.println("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        client = CosmosClient.builder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .connectionPolicy(ConnectionPolicy.defaultPolicy())
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .build();

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        Family andersenFamily = Families.getAndersenFamilyItem();
        Family wakefieldFamily = Families.getWakefieldFamilyItem();

        ArrayList<Family> familiesToCreate = new ArrayList<>();
        familiesToCreate.add(andersenFamily);
        familiesToCreate.add(wakefieldFamily);

        createFamiliesAndWaitForCompletion(familiesToCreate);

        familiesToCreate = new ArrayList<>();
        familiesToCreate.add(Families.getJohnsonFamilyItem());
        familiesToCreate.add(Families.getSmithFamilyItem());

        CountDownLatch createItemsCompletionLatch = new CountDownLatch(1);

        System.out.println("Creating items async and registering listener for the completion.");
        createFamiliesAsyncAndRegisterListener(familiesToCreate, createItemsCompletionLatch);

        CountDownLatch queryCompletionLatch = new CountDownLatch(1);

        System.out.println("Querying items async and registering listener for the result.");
        executeSimpleQueryAsyncAndRegisterListenerForResult(queryCompletionLatch);

        // as createFamiliesAsyncAndRegisterListener starts the operation in background
        // and only registers a listener, we used the createItemsCompletionLatch
        // to ensure we wait for the completion
        createItemsCompletionLatch.await();

        // as executeSimpleQueryAsyncAndRegisterListenerForResult starts the operation in background
        // and only registers a listener, we used the queryCompletionLatch
        // to ensure we wait for the completion
        queryCompletionLatch.await();
    }

    private void createDatabaseIfNotExists() throws Exception {
        writeToConsoleAndPromptToContinue(
                "Check if database " + databaseName + " exists.");

        Mono<CosmosDatabaseResponse> databaseReadObs = client.getDatabase(databaseName).read();

        Mono<CosmosDatabaseResponse> databaseExistenceObs = databaseReadObs
                .doOnNext(x -> {
                    System.out.println("database " + databaseName + " already exists.");
                })
                .onErrorResume(
                        e -> {
                            // if the database doesn't already exists
                            // readDatabase() will result in 404 error
                            if (e instanceof CosmosClientException) {
                                CosmosClientException de = (CosmosClientException) e;
                                // if database
                                if (de.statusCode() == 404) {
                                    // if the database doesn't exist, create it.
                                    System.out.println("database " + databaseName + " doesn't existed,"
                                            + " creating it...");

                                    CosmosDatabaseProperties dbDefinition = new CosmosDatabaseProperties(databaseName);

                                    return client.createDatabase(dbDefinition, null);
                                }
                            }

                            // some unexpected failure in reading database happened.
                            // pass the error up.
                            System.err.println("Reading database " + databaseName + " failed.");
                            return Mono.error(e);
                        });


        // wait for completion,
        // as waiting for completion is a blocking call try to
        // provide your own scheduler to avoid stealing netty io threads.
        database = databaseExistenceObs.block().database();

        System.out.println("Checking database " + databaseName + " completed!\n");
    }

    private void createContainerIfNotExists() throws Exception {
        writeToConsoleAndPromptToContinue(
                "Check if container " + containerName + " exists.");

        // query for a container with a given id
        // if it exists nothing else to be done
        // if the container doesn't exist, create it.

        // if there is no matching container create the container.

        container = database.queryContainers(
                new SqlQuerySpec("SELECT * FROM r where r.id = @id",
                        new SqlParameterList(
                                new SqlParameter("@id", containerName))), null)
                .single() // we know there is only single page of result (empty or with a match)
                .flatMap(page -> {
                    if (page.results().isEmpty()) {
                        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
                        ArrayList<String> partitionKeyPaths = new ArrayList<String>();
                        partitionKeyPaths.add("/id");
                        partitionKeyDefinition.paths(partitionKeyPaths);

                        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, partitionKeyDefinition);
                        System.out.println("Creating container " + containerName);

                        return database.createContainer(containerProperties);
                    } else {
                        // container already exists, nothing else to be done.
                        System.out.println("Container " + containerName + "already exists");
                        return Mono.empty();
                    }
                }).block().container();

        System.out.println("Checking container " + containerName + " completed!\n");
    }

    private void createFamiliesAsyncAndRegisterListener(List<Family> families, CountDownLatch completionLatch) {

        List<Mono<CosmosItemResponse>> createItemsOBs = new ArrayList<>();
        for (Family family : families) {
            Mono<CosmosItemResponse> obs = container.createItem(family);
            createItemsOBs.add(obs);
        }

        Flux.merge(createItemsOBs)
            .map(CosmosItemResponse::requestCharge)
            .reduce((sum, value) -> sum + value)
            .subscribe(
                    totalRequestCharge -> {
                        // this will get print out when completed
                        System.out.println("total charge for creating items is "
                                + totalRequestCharge);
                    },

                    // terminal error signal
                    e -> {
                        e.printStackTrace();
                        completionLatch.countDown();
                    },

                    // terminal completion signal
                    () -> {
                        completionLatch.countDown();
                    });
    }

    private void createFamiliesAndWaitForCompletion(List<Family> families) throws Exception {

        List<Mono<CosmosItemResponse>> createItemsOBs = new ArrayList<>();
        for (Family family : families) {
            Mono<CosmosItemResponse> obs = container.createItem(family);
            createItemsOBs.add(obs);
        }

        Double totalRequestCharge = Flux.merge(createItemsOBs)
            .map(CosmosItemResponse::requestCharge)
            .publishOn(scheduler)
            .map(charge -> {
                // as we don't want to run heavyWork() on netty IO thread, we provide the custom scheduler
                // for switching from netty IO thread to user thread.
                heavyWork();
                return charge;
            })
            .reduce((sum, value) -> sum + value)
            .block();

        writeToConsoleAndPromptToContinue(String.format("Created %d items with total request charge of %.2f",
                families.size(),
                totalRequestCharge));
    }

    private void heavyWork() {
        // I may do a lot of IO work: e.g., writing to log files
        // a lot of computational work
        // or may do Thread.sleep()

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (Exception e) {
        }
    }

    private void executeSimpleQueryAsyncAndRegisterListenerForResult(CountDownLatch completionLatch) {
        // Set some common query options
        FeedOptions queryOptions = new FeedOptions();
        queryOptions.maxItemCount(10);
        queryOptions.enableCrossPartitionQuery(true);

        Flux<FeedResponse<CosmosItemProperties>> queryObservable = container.queryItems(
                "SELECT * FROM Family WHERE Family.lastName != 'Andersen'", queryOptions);

        queryObservable
                .publishOn(scheduler)
                .subscribe(
                        page -> {
                            // we want to make sure heavyWork() doesn't block any of netty IO threads
                            // so we use observeOn(scheduler) to switch from the netty thread to user's thread.
                            heavyWork();

                            System.out.println("Got a page of query result with " +
                                    page.results().size() + " item(s)"
                                    + " and request charge of " + page.requestCharge());


                            System.out.println("Item Ids " + page.results().stream().map(d -> d.id())
                                    .collect(Collectors.toList()));
                        },
                        // terminal error signal
                        e -> {
                            e.printStackTrace();
                            completionLatch.countDown();
                        },

                        // terminal completion signal
                        () -> {
                            completionLatch.countDown();
                        });
    }

    private void writeToConsoleAndPromptToContinue(String text) throws IOException {
        System.out.println(text);
        System.out.println("Press any key to continue ...");
        System.in.read();
    }
}
