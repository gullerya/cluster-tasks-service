## Changelog

##### 3.6
- fixed [Issue #12](https://github.com/gullerya/cluster-tasks-service/issues/12) - fixed a behavior of scheduled tasks when submitted without interval (run too often)
- updated dependencies versions
- moves Spring's bean to component-scan flavor - allows to import the XML now without duplicate singletons' creation

##### 3.5
- fixed [Issue #11](https://github.com/gullerya/cluster-tasks-service/issues/11) - getting rid of static counters of Prometheus, making problems in multi class loaders environments

##### 3.4
- updating dependencies to the latest versions

##### 3.3
- fixed [Issue #5](https://github.com/gullerya/cluster-tasks-service/issues/5) - fixing the time zoning problem in PostgreSQL data provider

##### 3.2
- fixed [Issue #3](https://github.com/gullerya/cluster-tasks-service/issues/3) - fix the printing of the abandoned tasks' bodies (those with no tasks metadata) during the maintenance work
- fixed [Issue #4](https://github.com/gullerya/cluster-tasks-service/issues/4) - clean up the abandoned tasks' bodies even when the bodies partition is not being truncated due to the existence of non-abandoned bodies there

##### 3.1
- fixed [Issue #1](https://github.com/gullerya/cluster-tasks-service/issues/1) - prevent print out of a task bodies (might be quite big ones)

##### 3.0
- re-branded version of `GullerYA` (initial developer and library's owner) due to fork from `MicroFocus` repository

##### 2.2
- fixed [Issue #18](https://github.com/MicroFocus/cluster-tasks-service/issues/18) - added robustness to the queue working cycle

##### 2.1
- several performance improvements in queries
- several performance and algorithm improvements in fair tasks distribution logic

##### 2.0
- implemented [Issue #15](https://github.com/MicroFocus/cluster-tasks-service/issues/15) - added application context (`application-key`) as an optional attribute of the tasks
- implemented [Issue #16](https://github.com/MicroFocus/cluster-tasks-service/issues/16) - allowed conditional task dispatch based on consumer application logic (provided with `application-key`)

##### 1.8
- implemented [Issue #10](https://github.com/MicroFocus/cluster-tasks-service/issues/10)

##### 1.7
- fixed [Issue #6](https://github.com/MicroFocus/cluster-tasks-service/issues/6)
- fixed [Issue #7](https://github.com/MicroFocus/cluster-tasks-service/issues/7)

##### 1.6
- fixed [Issue #3](https://github.com/MicroFocus/cluster-tasks-service/issues/3)
- fixed [Issue #4](https://github.com/MicroFocus/cluster-tasks-service/issues/4)
- fixed [Issue #5](https://github.com/MicroFocus/cluster-tasks-service/issues/5)

##### 1.5
- improved detection and removal of staled tasks
- added tracking of active nodes
- improved overall DB related performance (pessimistic locking, but custom locks in SQL Server and PostgreSQL)

##### 1.4.0
- changed to public open-source repo
- added Apache 2.0 license
- added license notice for each file as required by OSRB team
- adjusted `pom.xml` to be compliant with Maven Central projects requirements
- added Java Docs generation option

##### 1.3.0
- added support for `PostgreSQL`

##### 1.2.0
- improved performance of SQL strings handling (static constants instead of dynamically building each time)
- removed usage of `CTSKM_IDX_2` in `SELECT` statements of MSSQL to reduce the probability of deadlocks

##### 1.1.9
- added `isEnabled` to SPI to allow configurer to suspend `cluster-tasks-service` from dispatching tasks and maintenance

##### 1.1.8
- added pending tasks counters gauge to enable monitoring for the per-processor-type queues debt

##### 1.1.7
- add schema readiness verification on startup

##### 1.1.6
- internal refactor as preparation for `PostgreSQL` support
- moving to latest `octane-component-parent` to get latest `fasterxml` library - previous ones has security issue 

##### 1.1.4
- changed naming of `ClusterTasksProcessorDefault` to `ClusterTasksProcessorSimple` in attempt to make it clearer
- removed unneeded attempt to enqueue scheduled tasks where there is high probability that there is already one in place

##### 1.1.3 (LTS release)
- fixed rare race condition that may have caused duplicate tasks runs in Oracle
- dispatching/GC logic improvements

##### 1.1.2
- GC improvements
- better scheduled tasks handling and recovery

##### 1.1.1
- minor fixes

##### 1.1.0
- documentation
- solve potential task loss due to race when (re-)enqueueing task with uniqueness key
- tasks creation refactored, now providing builders for convenience, early validation and coding-time restrictions enforcement

##### 1.0.9 (LTS release)
- added fairness logic between the concurrency keys
- added integration with `prometheus` monitoring metrics

##### 1.0.8
- small improvements in tasks retrieving query
- fixed potential deadlock while task retrieved in MSSQL use-case

##### 1.0.7
- added tests coverage monitoring (`jacoco-coverage` Maven profile from parent pom)
- moved maintenance task back to be a thread and not CTP task due to defect in case when this task staled on running
- sequencing added (becoming fully independent from hosting application)
- flyway DB management added, achieving independency from hosting application (the only prerequisite left as of now is to get valid DataSource object)
- API changes - not a single ClusterTask object anymore, but one for enqueue and one (immutable) when given out to processor 

##### 1.0.5 (LTS release)
- initial version
