SELECT *
FROM AIRCRAFTS,CERTIFIED,SCHEDULE
WHERE AIRCRAFTS.aid=CERTIFIED.aid,AIRCRAFTS.aid=SCHEDULE.aid,AIRCRAFTS.cruisingrange>"5000",eid>"5000"