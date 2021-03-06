CREATE TABLE FLIGHTS (
    flno INTEGER, 
    camefrom VARCHAR(32),
    goingto VARCHAR(32),
    distance INTEGER,
    departs INTEGER,
    arrives INTEGER,
    PRIMARY KEY (flno)
);

CREATE TABLE SCHEDULE (
    flno INTEGER,
    aid INTEGER,
    PRIMARY KEY (flno, aid)
);

CREATE TABLE EMPLOYEES (
    eid INTEGER,
    ename VARCHAR(32),
    salary INTEGER,
    PRIMARY KEY (eid)
);

CREATE TABLE CERTIFIED (
    eid INTEGER,
    aid INTEGER,
    PRIMARY KEY (eid, aid)
);

CREATE TABLE AIRCRAFTS (
    aid INTEGER,
    aname varchar(32),
    cruisingrange INTEGER,
    PRIMARY KEY(aid)
);