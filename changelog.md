###Changelog

#####1.0.7 (current)
- added tests coverage monitoring (`jacoco-coverage` Maven profile from parent pom)
- moved GC task back to be a thread and not CTP task due to defect in case when this task staled on running
- sequencing added (becoming fully independent from hosting application)
- flyway DB management added, achieving independency from hosting application (the only prerequisite left as of now is to get valid DataSource object)
- API changes - not a single ClusterTask object anymore, but one for enqueue and one (immutable) when given out to processor 

#####1.0.5
- initial version
