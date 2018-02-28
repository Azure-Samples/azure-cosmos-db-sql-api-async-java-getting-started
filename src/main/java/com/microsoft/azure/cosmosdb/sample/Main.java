/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

import rx.Observable;

public class Main {

    private AsyncDocumentClient client;

    private String databaseName = "AzureSampleFamilyDB";
    private String collectionName = "FamilyCollection";

    /**
     * Run a Hello DocumentDB console application.
     * 
     * @param args
     *            command line arguments
     * @throws DocumentClientException
     *             exception
     * @throws IOException 
     */
    public static void main(String[] args) {

        Main p = new Main();

        try {  
            p.getStartedDemo();
            System.out.println(String.format("Demo complete, please hold while resources are deleted"));
        } catch (Exception e) {
            System.out.println(String.format("DocumentDB GetStarted failed with %s", e));
        } finally {
            System.out.println("close the client");
            p.client.close();
            System.exit(0);
        }
    }

    private void getStartedDemo() throws Exception {

        System.out.println("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        client = new AsyncDocumentClient.Builder()
                .withServiceEndpoint(AccountSettings.HOST)
                .withMasterKey(AccountSettings.MASTER_KEY)
                .withConnectionPolicy(ConnectionPolicy.GetDefault())
                .withConsistencyLevel(ConsistencyLevel.Session)
                .build();

        this.createDatabase();
        this.createDocumentCollection();

        Family andersenFamily = Families.getAndersenFamilyDocument();
        Family wakefieldFamily = Families.getWakefieldFamilyDocument();

        ArrayList<Family> familiesToCreate = new ArrayList<>();
        familiesToCreate.add(andersenFamily);
        familiesToCreate.add(wakefieldFamily);

        createFamiliesAndWaitForCompletion(familiesToCreate);


        familiesToCreate = new ArrayList<>();
        familiesToCreate.add(Families.getJohnsonFamilyDocument());
        familiesToCreate.add(Families.getSmithFamilyDocument());

        System.out.println("create documents async and registering listener for completion");
        createFamiliesAsyncAndRegisterListener(familiesToCreate);

        System.out.println("query documents async and registering listener for result");
        executeSimpleQueryAsyncAndRegisterListenerForResult();

        // just wait till async work finish
        Thread.sleep(10000);

    }

    private void createDatabase() throws Exception {
        String databaseLink = String.format("/dbs/%s", databaseName);

        // Attempts to create a database with name database name
        // if it already exists will delete it
        // then re-create it

        Database database = new Database();
        database.setId(databaseName);

        Observable<ResourceResponse<Database>> 
        resourceResponseObs = client.createDatabase(database, null);

        resourceResponseObs = resourceResponseObs.onErrorResumeNext(
                e -> {
                    // in case of error check if the failure is due to a database with same name exists

                    if (e instanceof DocumentClientException) {
                        DocumentClientException de = (DocumentClientException) e;
                        // if database 
                        if (de.getStatusCode() == 409) {
                            System.out.println("database already existed,"
                                    + " deleting and recreating it");
                            return client.deleteDatabase(databaseLink, null)
                                    .concatWith(
                                            client.createDatabase(database, null));
                        }
                    }

                    // else
                    // unexpected failure
                    return Observable.error(e);
                });

        resourceResponseObs.toCompletable().await();

        writeToConsoleAndPromptToContinue("Created database " + databaseName);
    }

    private void createDocumentCollection() throws Exception {
        String databaseLink = String.format("/dbs/%s", databaseName);

        DocumentCollection collection = new DocumentCollection();
        collection.setId(collectionName);
        Observable<ResourceResponse<DocumentCollection>> obs = client.createCollection(databaseLink, collection, null);

        obs.toCompletable().await();
        writeToConsoleAndPromptToContinue("Created collection " + collectionName);
    }

    private void createFamiliesAsyncAndRegisterListener(List<Family> families) throws Exception {

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        List<Observable<ResourceResponse<Document>>> createDocumentsOBs = new ArrayList<>();
        for(Family family: families) {
            Observable<ResourceResponse<Document>> obs = client.createDocument(
                    collectionLink, family, new RequestOptions(), true);
            createDocumentsOBs.add(obs);
        }

        Observable.merge(createDocumentsOBs)
        .map(ResourceResponse::getRequestCharge)
        .reduce((sum, value) -> sum + value)
        .subscribe(totalRequestCharge -> {
            // this will get print out when completed
            System.out.println("total charge for creating documents is "
                    + totalRequestCharge);
        });

    }

    private void createFamiliesAndWaitForCompletion(List<Family> families) throws Exception {

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        List<Observable<ResourceResponse<Document>>> createDocumentsOBs = new ArrayList<>();
        for(Family family: families) {
            Observable<ResourceResponse<Document>> obs = client.createDocument(
                    collectionLink, family, new RequestOptions(), true);
            createDocumentsOBs.add(obs);
        }

        Double totalRequestCharge = Observable.merge(createDocumentsOBs)
                .map(ResourceResponse::getRequestCharge)
                .reduce((sum, value) -> sum+value)
                .toBlocking().single();

        writeToConsoleAndPromptToContinue(String.format("Created %d documents with total request charge of %.2f",
                families.size(),
                totalRequestCharge));
    }

    private void executeSimpleQueryAsyncAndRegisterListenerForResult() {
        // Set some common query options
        FeedOptions queryOptions = new FeedOptions();
        queryOptions.setMaxItemCount(100);
        queryOptions.setEnableCrossPartitionQuery(true);

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        Observable<FeedResponse<Document>> queryObservable = 
                client.queryDocuments(collectionLink,
                        "SELECT * FROM Family WHERE Family.lastName = 'Andersen'", queryOptions);

        queryObservable.subscribe(
                queryResultPage -> {
                    System.out.println("Got a page of query result with " +
                            queryResultPage.getResults().size() + " document(s)"
                            + " and request charge of " + queryResultPage.getRequestCharge());
                });
    }

    private void writeToConsoleAndPromptToContinue(String text) throws IOException {
        System.out.println(text);
        System.out.println("Press any key to continue ...");
        System.in.read();
    }
}
