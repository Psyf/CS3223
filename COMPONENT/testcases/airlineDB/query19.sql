SELECT AVG(FLIGHTS.distance)
FROM FLIGHTS,SCHEDULE
WHERE FLIGHTS.flno=SCHEDULE.flno