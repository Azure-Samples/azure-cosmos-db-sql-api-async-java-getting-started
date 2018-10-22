---
services: cosmos-db
platforms: java
author: moderakh
---

# Java app using Azure Cosmos DB Async Java SDK

Azure Cosmos DB is a globally distributed multi-model database. One of the supported APIs is the SQL API, which provides a JSON document model with SQL querying and JavaScript procedural logic. This sample shows you how to use the Azure Cosmos DB with the SQL API to store and access data from a Java application.

## Getting Started

### Prerequisites

* Before you can run this sample, you must have the following prerequisites:

   * An active Azure account. If you don't have one, you can sign up for a [free account](https://azure.microsoft.com/free/). Alternatively, you can use the [Azure Cosmos DB Emulator](https://azure.microsoft.com/documentation/articles/documentdb-nosql-local-emulator) for this tutorial. As emulator https certificate is self signed, you need to import its certificate to java trusted cert store as [explained here](https://docs.microsoft.com/en-us/azure/cosmos-db/local-emulator-export-ssl-certificates).

   * JDK 1.8+
   * Maven

### Quickstart

* Then, clone this repository using

```bash
git clone https://github.com/Azure-Samples/azure-cosmos-db-sql-api-async-java-getting-started.git
```

* From a command prompt or shell, run the following command to compile and resolve dependencies.

```bash
cd azure-cosmos-db-sql-api-async-java-getting-started
cd azure-cosmosdb-get-started
mvn package
```

* From a command prompt or shell, run the following command to run the application.

```bash
mvn exec:java -DACCOUNT_HOST=YOUR_COSMOS_DB_HOSTNAME -DACCOUNT_KEY=YOUR_COSMOS_DB_MASTER_KEY
```

## About the code

The code included in this sample is intended to get you quickly started with a Java application that connects to Azure Cosmos DB with the SQL API.

## More information

- [Azure Cosmos DB](https://docs.microsoft.com/azure/cosmos-db/introduction)
- [Azure Cosmos DB : SQL API](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-api-introduction)
- [Async Java SDK Github for SQL API of Azure Cosmos DB](https://github.com/Azure/azure-cosmosdb-java)
- [Async Java SDK JavaDoc for SQL API of Azure Cosmos DB](https://azure.github.io/azure-cosmosdb-java)
