SELECT FLIGHTS.flno,FLIGHTS.from,FLIGHTS.to,FLIGHTS.distance
FROM AIRCRAFTS,CERTIFIED,SCHEDULE
WHERE AIRCRAFTS.aid=CERTIFIED.aid,AIRCRAFTS.aid=SCHEDULE.aid
ORDERBY AIRCRAFTS.aid DESC