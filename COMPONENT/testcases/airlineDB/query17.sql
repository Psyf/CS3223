SELECT COUNT(AIRCRAFTS.cruisingrange)
FROM AIRCRAFTS,SCHEDULE
WHERE AIRCRAFTS.aid=SCHEDULE.aid