###Changelog

#####1.0.7 (current)
- added tests coverage monitoring (`coverage` Maven profile)
- moved GC task back to be a thread and not CTP task due to defect in case when this task staled on running

#####1.0.6
- sequencing added (becoming fully independent from hosting application)
- flyway DB management added, achieving independency from hosting application (the only prerequisite left as of now is to get valid DataSource object) 

#####1.0.5
- initial version
