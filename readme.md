[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/com.gullerya/cluster-tasks-service.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.gullerya%22%20AND%20a:%22cluster-tasks-service%22)
[![Travis branch](https://img.shields.io/travis/gullerya/cluster-tasks-service/master.svg?logo=travis)](https://travis-ci.org/gullerya/cluster-tasks-service/branches)
[![AppVeyor branch](https://img.shields.io/appveyor/ci/gullerya/cluster-tasks-service/master.svg?logo=appveyor)](https://ci.appveyor.com/project/gullerya/cluster-tasks-service/branch/master)
[![codecov](https://codecov.io/gh/gullerya/cluster-tasks-service/branch/master/graph/badge.svg)](https://codecov.io/gh/gullerya/cluster-tasks-service)
[![Codacy branch grade](https://img.shields.io/codacy/grade/bb93a604180e450e85dfbcca743fbea0/master.svg?logo=Codacy)](https://www.codacy.com/app/gullerya/cluster-tasks-service)

## Summary

`cluster-tasks-service` library is built to provide distribution of tasks across a clustered environment.
Beside providing basic queue functionality in a clustered environment, `cluster-tasks-service` (henceforth `CTS`) employs several advanced distribution features.

Most significant feature, the one that `CTS` was originally written for, is an ability to control tasks processing in a __channelled__ fashion, where only a single task from a specific __channel__ will run at any given moment in the whole cluster.

Important: this version of `CTS` is my own fork from the `MicroFocus` repository, where I've developed the library first as an employee. Present fork represents my own work since the moment I've left the company and any mention of `MicroFocus` anywhere is for historical reason. `MicroFocus` is not to be held responsible for anything happening to `CTS` in the present fork.

##### Documentation TOC:
- [Configurer SPI](docs/cts-configurer-spi.md) - wiring library into consuming application
- [CTS service](docs/cts-service-api.md) - main library entry point
- [Task processors](docs/cts-task-processors.md) - types and extensibility points
- [Tasks](docs/cts-tasks-overview.md) - types, attributes, behaviors
- [Monitoring](docs/monitoring.md) - metrics and logs
- [Examples](docs/cts-examples.md)
- [Changelog](docs/changelog.md) 

### Importing and initialization of the service (done once per application instance)

`CTS` is Spring based. Please follow the steps below to plug the library into your application lifecycle:

1. Setup the dependency in your `pom.xml` (as in example below) or other compatible build tool configuration (you may review the [Changelog](docs/changelog.md) for an available versions):
    ```
    <dependency>
        <artifactId>cluster-tasks-service</artifactId>
        <groupId>com.gullerya</groupId>
        <version>${relevant.cts.version}</version>
    </dependency>
    ```

2. Make `CTS`'s Spring beans available to your application. This could be done by either adding `cluster-tasks-service-context.xml` file to the list of your context path's in Java driven context initialization, or importing it from one of your own Spring XMLs, like in example below:
    ```
    <import resource="classpath:cluster-tasks-service-context.xml"/>
    ```

3. Implement `ClusterTasksServiceConfigurerSPI` interface and define your implementation as a __Spring__ bean (singleton).
Upon application start, `CTS` will wire this bean and take everything it needs from it (DataSource object, for example).
See [CTS configuration SPI overview](docs/cts-configurer-spi.md) for more details.
  
If these steps done right, `CTS` will kick in upon the application start and be available for submitting and processing tasks.

## Concepts

The world of `CTS` may roughly be separated into two:
- the `CTS` __service__ itself is responsible for its environment setup, ongoing work and maintenance: collecting and registering processors, pulling and handing tasks over, statistics and monitoring, maintenance of finished/dead tasks etc.
 Service also provides few generic API for the consumer, for example API to enqueue tasks.
 Service import/bootstrapping is a __one-time__ effort per application.
- a __processors__ (aka Cluster Tasks Processor, aka CTP), implemented by library consumer and extending appropriate base abstract class, are an actual tasks processors with custom business logic, almost completely transparent to the framework.
 It is likely, that there will be many __processors__ (CPTs) in your application, each handling specific use-case.
 Adding processors is an ongoing effort, aligned with the application evolution.
 
 ## Schema management
 
 `CTS` knows to handle the schema automatically (create/upgrade) or rely on the provided one. While the second flavor looks highly fragile and non-convenient to me, I've left that option open for a special cases.
 
 If you'd like `CTS` to manage it's own scheme, you should provide a relevant `DataSource` object in `getAdministrativeDataSource` API of your configurer implementation.
 
 Otherwise, you may not implement that method (or return `null` from it) and then `CTS` will assume that the hosting application is managing the schema for it.
 
 > While auto schema management has no limitations for Oracle and PostgreSQL, SQL Server's lowest version is SqlServer2016 (inclusive). Shall you have a version of SQL Server prior to that, unfortunately you'll need to perform the schema maintenance your self. Yet, schema management SQLs found in `CTS` sources may vastly simplify this task.