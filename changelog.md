###Changelog

#####1.3.1 (current snapshot)

#####1.3.0
- added support for `PostgreSQL`

#####1.2.0
- improved performance of SQL strings handling (static constants instead of dynamically building each time)
- removed usage of `CTSKM_IDX_2` in `SELECT` statements of MSSQL to reduce the probability of deadlocks

#####1.1.9
- added `isEnabled` to SPI to allow configurer to suspend `cluster-tasks-service` from dispatching tasks and maintenance

#####1.1.8
- added pending tasks counters gauge to enable monitoring for the per-processor-type queues debt

#####1.1.7
- add schema readiness verification on startup

#####1.1.6
- internal refactor as preparation for `PostgreSQL` support
- moving to latest `octane-component-parent` to get latest `fasterxml` library - previous ones has security issue 

#####1.1.4
- changed naming of `ClusterTasksProcessorDefault` to `ClusterTasksProcessorSimple` in attempt to make it clearer
- removed unneeded attempt to enqueue scheduled tasks where there is high probability that there is already one in place

#####1.1.3 (LTS release)
- fixed rare race condition that may have caused duplicate tasks runs in Oracle
- dispatching/GC logic improvements

#####1.1.2
- GC improvements
- better scheduled tasks handling and recovery

#####1.1.1
- minor fixes

#####1.1.0
- documentation
- solve potential task loss due to race when (re-)enqueueing task with uniqueness key
- tasks creation refactored, now providing builders for convenience, early validation and coding-time restrictions enforcement

#####1.0.9 (LTS release)
- added fairness logic between the concurrency keys
- added integration with `prometheus` monitoring metrics

#####1.0.8
- small improvements in tasks retrieving query
- fixed potential deadlock while task retrieved in MSSQL use-case

#####1.0.7
- added tests coverage monitoring (`jacoco-coverage` Maven profile from parent pom)
- moved maintenance task back to be a thread and not CTP task due to defect in case when this task staled on running
- sequencing added (becoming fully independent from hosting application)
- flyway DB management added, achieving independency from hosting application (the only prerequisite left as of now is to get valid DataSource object)
- API changes - not a single ClusterTask object anymore, but one for enqueue and one (immutable) when given out to processor 

#####1.0.5 (LTS release)
- initial version
