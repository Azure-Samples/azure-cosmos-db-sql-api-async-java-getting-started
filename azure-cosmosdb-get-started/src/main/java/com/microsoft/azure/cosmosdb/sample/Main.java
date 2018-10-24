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

import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private final ExecutorService executorService;
    private final Scheduler scheduler;

    private AsyncDocumentClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String collectionName = "FamilyCollection";

    public Main() {
        executorService = Executors.newFixedThreadPool(100);
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
        scheduler = Schedulers.from(executorService);
    }

    public void close() {
        executorService.shutdown();
        client.close();
    }

    /**
     * Run a Hello DocumentDB console application.
     *
     * @param args command line args.
     */
    public static void main(String[] args) {
        Main p = new Main();

        try {
            p.getStartedDemo();
            System.out.println(String.format("Demo complete, please hold while resources are deleted"));
        } catch (Exception e) {
            System.err.println(String.format("DocumentDB GetStarted failed with %s", e));
        } finally {
            System.out.println("close the client");
            p.close();
        }
    }

    private void getStartedDemo() throws Exception {
        System.out.println("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        client = new AsyncDocumentClient.Builder()
                .withServiceEndpoint(AccountSettings.HOST)
                .withMasterKeyOrResourceToken(AccountSettings.MASTER_KEY)
                .withConnectionPolicy(ConnectionPolicy.GetDefault())
                .withConsistencyLevel(ConsistencyLevel.Session)
                .build();

        createDatabaseIfNotExists();
        createDocumentCollectionIfNotExists();

        Family andersenFamily = Families.getAndersenFamilyDocument();
        Family wakefieldFamily = Families.getWakefieldFamilyDocument();

        ArrayList<Family> familiesToCreate = new ArrayList<>();
        familiesToCreate.add(andersenFamily);
        familiesToCreate.add(wakefieldFamily);

        createFamiliesAndWaitForCompletion(familiesToCreate);

        familiesToCreate = new ArrayList<>();
        familiesToCreate.add(Families.getJohnsonFamilyDocument());
        familiesToCreate.add(Families.getSmithFamilyDocument());

        CountDownLatch createDocumentsCompletionLatch = new CountDownLatch(1);

        System.out.println("Creating documents async and registering listener for the completion.");
        createFamiliesAsyncAndRegisterListener(familiesToCreate, createDocumentsCompletionLatch);

        CountDownLatch queryCompletionLatch = new CountDownLatch(1);

        System.out.println("Querying documents async and registering listener for the result.");
        executeSimpleQueryAsyncAndRegisterListenerForResult(queryCompletionLatch);

        // as createFamiliesAsyncAndRegisterListener starts the operation in background
        // and only registers a listener, we used the createDocumentsCompletionLatch
        // to ensure we wait for the completion
        createDocumentsCompletionLatch.await();

        // as executeSimpleQueryAsyncAndRegisterListenerForResult starts the operation in background
        // and only registers a listener, we used the queryCompletionLatch
        // to ensure we wait for the completion
        queryCompletionLatch.await();
    }

    private void createDatabaseIfNotExists() throws Exception {
        writeToConsoleAndPromptToContinue(
                "Check if database " + databaseName + " exists.");

        String databaseLink = String.format("/dbs/%s", databaseName);

        Observable<ResourceResponse<Database>> databaseReadObs =
                client.readDatabase(databaseLink, null);

        Observable<ResourceResponse<Database>> databaseExistenceObs =
                databaseReadObs
                        .doOnNext(x -> {
                            System.out.println("database " + databaseName + " already exists.");
                        })
                        .onErrorResumeNext(
                                e -> {
                                    // if the database doesn't already exists
                                    // readDatabase() will result in 404 error
                                    if (e instanceof DocumentClientException) {
                                        DocumentClientException de = (DocumentClientException) e;
                                        // if database
                                        if (de.getStatusCode() == 404) {
                                            // if the database doesn't exist, create it.
                                            System.out.println("database " + databaseName + " doesn't existed,"
                                                    + " creating it...");

                                            Database dbDefinition = new Database();
                                            dbDefinition.setId(databaseName);

                                            return client.createDatabase(dbDefinition, null);
                                        }
                                    }

                                    // some unexpected failure in reading database happened.
                                    // pass the error up.
                                    System.err.println("Reading database " + databaseName + " failed.");
                                    return Observable.error(e);
                                });


        // wait for completion,
        // as waiting for completion is a blocking call try to
        // provide your own scheduler to avoid stealing netty io threads.
        databaseExistenceObs.toCompletable().await();

        System.out.println("Checking database " + databaseName + " completed!\n");
    }

    private void createDocumentCollectionIfNotExists() throws Exception {
        writeToConsoleAndPromptToContinue(
                "Check if collection " + collectionName + " exists.");

        // query for a collection with a given id
        // if it exists nothing else to be done
        // if the collection doesn't exist, create it.

        String databaseLink = String.format("/dbs/%s", databaseName);

        client.queryCollections(databaseLink,
                new SqlQuerySpec("SELECT * FROM r where r.id = @id",
                        new SqlParameterCollection(
                                new SqlParameter("@id", collectionName))), null)
                .single() // we know there is only single page of result (empty or with a match)
                .flatMap(page -> {
                    if (page.getResults().isEmpty()) {
                        // if there is no matching collection create the collection.
                        DocumentCollection collection = new DocumentCollection();
                        collection.setId(collectionName);
                        System.out.println("Creating collection " + collectionName);

                        return client.createCollection(databaseLink, collection, null);
                    } else {
                        // collection already exists, nothing else to be done.
                        System.out.println("Collection " + collectionName + "already exists");
                        return Observable.empty();
                    }
                }).toCompletable().await();

        System.out.println("Checking collection " + collectionName + " completed!\n");
    }

    private void createFamiliesAsyncAndRegisterListener(List<Family> families, CountDownLatch completionLatch) {

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        List<Observable<ResourceResponse<Document>>> createDocumentsOBs = new ArrayList<>();
        for (Family family : families) {
            Observable<ResourceResponse<Document>> obs = client.createDocument(
                    collectionLink, family, new RequestOptions(), true);
            createDocumentsOBs.add(obs);
        }

        Observable.merge(createDocumentsOBs)
                .map(ResourceResponse::getRequestCharge)
                .reduce((sum, value) -> sum + value)
                .subscribe(
                        totalRequestCharge -> {
                            // this will get print out when completed
                            System.out.println("total charge for creating documents is "
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
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        List<Observable<ResourceResponse<Document>>> createDocumentsOBs = new ArrayList<>();
        for (Family family : families) {
            Observable<ResourceResponse<Document>> obs = client.createDocument(
                    collectionLink, family, new RequestOptions(), true);
            createDocumentsOBs.add(obs);
        }

        Double totalRequestCharge = Observable.merge(createDocumentsOBs)
                .map(ResourceResponse::getRequestCharge)
                .observeOn(scheduler) // the scheduler will be used for the following work
                .map(charge -> {
                    // as we don't want to run heavyWork() on netty IO thread, we provide the custom scheduler
                    // for switching from netty IO thread to user thread.
                    heavyWork();
                    return charge;
                })
                .reduce((sum, value) -> sum + value)
                .toBlocking().single();

        writeToConsoleAndPromptToContinue(String.format("Created %d documents with total request charge of %.2f",
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
        queryOptions.setMaxItemCount(100);
        queryOptions.setEnableCrossPartitionQuery(true);

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        Observable<FeedResponse<Document>> queryObservable =
                client.queryDocuments(collectionLink,
                        "SELECT * FROM Family WHERE Family.lastName = 'Andersen'", queryOptions);

        queryObservable
                .observeOn(scheduler)
                .subscribe(
                        queryResultPage -> {
                            // we want to make sure heavyWork() doesn't block any of netty IO threads
                            // so we use observeOn(scheduler) to switch from the netty thread to user's thread.
                            heavyWork();

                            System.out.println("Got a page of query result with " +
                                    queryResultPage.getResults().size() + " document(s)"
                                    + " and request charge of " + queryResultPage.getRequestCharge());
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
