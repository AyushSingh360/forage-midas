# Midas Core

JP Morgan Chase & Co. - Advanced Software Engineering Job Simulation (Forage)

## Overview

Midas Core is a Spring Boot application built as part of the JPMC Advanced Software Engineering Forage program. The application processes financial transactions via Kafka, integrates with an incentives API, and exposes a REST API for querying user balances.

## Tech Stack

- **Java 17**
- **Spring Boot 3.2.5**
- **Apache Kafka** (messaging)
- **Spring Data JPA** (database)
- **H2 Database** (in-memory)
- **Testcontainers** (integration testing)

## Tasks Completed

### Task 1 - Project Setup
- Installed Java 17 and configured Maven dependencies
- Bootstrapped Spring Boot application with required dependencies

### Task 2 - Kafka Integration
- Implemented Kafka producer/consumer for transaction processing
- Configured JSON serialization/deserialization

### Task 3 - Transaction Validation & H2 Database
- Created `TransactionRecord` entity with many-to-one relationship to `UserRecord`
- Implemented transaction validation (sender/recipient existence, sufficient balance)
- Balanced adjustments applied on successful transactions

### Task 4 - Incentives API Integration
- Integrated external incentives API (port 8080)
- Added incentive amount to recipient balance after each valid transaction
- Created `Incentive` model and `RestTemplate` configuration

### Task 5 - REST API
- Exposed `/balance` endpoint on port 33400
- Returns user balance as JSON, defaults to 0 for non-existent users

## Project Structure

```
src/
├── main/java/com/jpmc/midascore/
│   ├── MidasCoreApplication.java
│   ├── component/
│   │   ├── DatabaseConduit.java
│   │   └── TransactionConsumer.java
│   ├── config/
│   │   └── AppConfig.java
│   ├── controller/
│   │   └── BalanceController.java
│   ├── entity/
│   │   ├── UserRecord.java
│   │   └── TransactionRecord.java
│   ├── foundation/
│   │   ├── Balance.java
│   │   ├── Incentive.java
│   │   └── Transaction.java
│   └── repository/
│       ├── UserRepository.java
│       └── TransactionRepository.java
└── test/java/com/jpmc/midascore/
    ├── TaskOneTests.java
    ├── TaskTwoTests.java
    ├── TaskThreeTests.java
    ├── TaskFourTests.java
    └── TaskFiveTests.java
```

## Running the Application

```bash
# Build
mvn clean install

# Run
mvn spring-boot:run

# Run tests
mvn test
```

## Forage Program

This project was completed as part of the [JPMC Advanced Software Engineering Virtual Experience](https://www.theforage.com/simulations/jpmorgan-advanced-software-engineering) on Forage.
