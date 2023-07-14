--creating table for first time readers

CREATE TABLE first_reader (
date_time_event TIMESTAMP,
event_type TEXT,
country TEXT,
user_id TEXT,
source VARCHAR,
topic TEXT);

--uploading data to the table

COPY first_reader FROM '/home/biologist/Dilan/first_read.csv' DELIMITER ';';

---split the timestamp to make it easier to work with the data

ALTER TABLE first_reader ADD COLUMN date_event date;
ALTER TABLE first_reader ADD COLUMN time_event time;

UPDATE first_reader
SET
    date_event = date_time_event::date,
    time_event = date_time_event::time;

ALTER TABLE first_reader DROP COLUMN date_time_event;  

--check the table

SELECT * FROM first_read
LIMIT 10;

--creating table for returning readers

CREATE TABLE returning_reader (
date_time_event TIMESTAMP,
event_type TEXT,
country TEXT,
user_id TEXT,
topic TEXT);

--uploading data to the table

COPY returning_reader FROM '/home/biologist/Dilan/returning_read.csv' DELIMITER ';';

---split the timestamp to make it easier to work with the data

ALTER TABLE returning_reader ADD COLUMN date_event date;
ALTER TABLE returning_reader ADD COLUMN time_event time;

UPDATE returning_reader
SET
    date_event = date_time_event::date,
    time_event = date_time_event::time;

ALTER TABLE returning_reader DROP COLUMN date_time_event;  

--check the table

SELECT * FROM returning_reader
LIMIT 10;

--creating table for subscribed readers

CREATE TABLE subscribed_reader (
date_time_event TIMESTAMP,
event_type TEXT,
user_id TEXT);

--uploading data to the table

COPY subscribed_reader FROM '/home/biologist/Dilan/subscribe.csv' DELIMITER ';';

---split the timestamp to make it easier to work with the data

ALTER TABLE subscribed_reader ADD COLUMN date_event date;
ALTER TABLE subscribed_reader ADD COLUMN time_event time;

UPDATE subscribed_reader
SET
    date_event = date_time_event::date,
    time_event = date_time_event::time;

ALTER TABLE subscribed_reader DROP COLUMN date_time_event;  

--check the table

SELECT * FROM subscribed_reader
LIMIT 10;

--creating table for costumers

CREATE TABLE spent_reader (
date_time_event TIMESTAMP,
event_type TEXT,
user_id TEXT,
price INTEGER);

--uploading data to the table

COPY spent_reader FROM '/home/biologist/Dilan/buy.csv' DELIMITER ';';

---split the timestamp to make it easier to work with the data

ALTER TABLE spent_reader ADD COLUMN date_event date;
ALTER TABLE spent_reader ADD COLUMN time_event time;

UPDATE spent_reader
SET
    date_event = date_time_event::date,
    time_event = date_time_event::time;

ALTER TABLE spent_reader DROP COLUMN date_time_event;  

--check the table

SELECT * FROM spent_reader
LIMIT 10;

---getting familiar with the data

SELECT date_event, COUNT (*)
FROM first_reader
GROUP BY date_event;

SELECT user_id, COUNT (*)
FROM returning_reader
GROUP BY user_id;

SELECT user_id, COUNT (*)
FROM subscribed_reader
GROUP BY user_id;

SELECT user_id, COUNT (*)
FROM spent_reader
GROUP BY user_id;

SELECT country, COUNT (*)
FROM first_reader
GROUP BY country
ORDER BY count DESC;

---COUNTRY ANALYSIS
--first_reader/country

SELECT country, COUNT (*)
FROM first_reader
GROUP BY country
ORDER BY country;

---returning reader/country 

SELECT country, COUNT (DISTINCT (user_id))
FROM returning_reader
GROUP BY country
ORDER BY country;

---returning reader/country all 

SELECT country, COUNT (*)
FROM returning_reader
GROUP BY country
ORDER BY country;

---subscribed reader/country

SELECT country, COUNT (*)
FROM first_reader
JOIN subscribed_reader
ON first_reader.user_id = subscribed_reader.user_id
GROUP BY country
ORDER BY country;

---costumers/country all

SELECT country, COUNT (*)
FROM first_reader
FULL JOIN spent_reader
ON first_reader.user_id = spent_reader.user_id
GROUP BY country
ORDER BY country;

---costumers/country 

SELECT country, COUNT (*)
FROM first_reader
JOIN spent_reader
ON first_reader.user_id = spent_reader.user_id
GROUP BY country
ORDER BY country;

--revenue by country

SELECT country, SUM (spent_reader.price)
FROM first_reader
FUll JOIN spent_reader
ON first_reader.user_id = spent_reader.user_id
GROUP BY country
ORDER BY country;


--revenue by country from video courses

SELECT country, SUM (spent_reader.price)
FROM first_reader
FUll JOIN spent_reader
ON first_reader.user_id = spent_reader.user_id
WHERE spent_reader.price > 8
GROUP BY country
ORDER BY country;

--revenue by country from books

SELECT country, SUM (spent_reader.price)
FROM first_reader
FUll JOIN spent_reader
ON first_reader.user_id = spent_reader.user_id
WHERE spent_reader.price < 9
GROUP BY country
ORDER BY country;

----revenue/country from boooks/video courses visualization in Google looker studio
  
SELECT revenue.country, revenue_all, video_course_revenue, book_revenue
FROM
  (SELECT country, SUM (spent_reader.price) AS revenue_all
  FROM first_reader
  FUll JOIN spent_reader
  ON first_reader.user_id = spent_reader.user_id
  GROUP BY country) AS revenue
FULL JOIN
  (SELECT country, SUM (spent_reader.price) AS video_course_revenue
  FROM first_reader
  FUll JOIN spent_reader
  ON first_reader.user_id = spent_reader.user_id
  WHERE spent_reader.price > 8
  GROUP BY country) AS video
ON revenue.country = video.country
FULL JOIN
  (SELECT country, SUM (spent_reader.price) AS book_revenue
  FROM first_reader
  FUll JOIN spent_reader
  ON first_reader.user_id = spent_reader.user_id
  WHERE spent_reader.price < 9
  GROUP BY country) AS book
ON video.country = book.country
ORDER BY country;


--SOURCE ANALYSIS
----- revenue/source

SELECT source, SUM (spent_reader.price)
FROM first_reader
JOIN spent_reader
ON first_reader.user_id = spent_reader.user_id
GROUP BY source
ORDER BY source;

----revenue/video/book/source  visualisation

SELECT revenue.source, revenue_all, video_course_revenue, book_revenue
FROM
  (SELECT source, SUM (spent_reader.price) AS revenue_all
  FROM first_reader
  FUll JOIN spent_reader
  ON first_reader.user_id = spent_reader.user_id
  GROUP BY source) AS revenue
FULL JOIN
  (SELECT source, SUM (spent_reader.price) AS video_course_revenue
  FROM first_reader
  FUll JOIN spent_reader
  ON first_reader.user_id = spent_reader.user_id
  WHERE spent_reader.price > 8
  GROUP BY source) AS video
ON revenue.source = video.source
FULL JOIN
  (SELECT source, SUM (spent_reader.price) AS book_revenue
  FROM first_reader
  FUll JOIN spent_reader
  ON first_reader.user_id = spent_reader.user_id
  WHERE spent_reader.price < 9
  GROUP BY source) AS book
ON video.source = book.source
ORDER BY source;


--costumers/source

SELECT source, COUNT (*)
FROM first_reader
JOIN spent_reader
ON first_reader.user_id = spent_reader.user_id
GROUP BY source
ORDER BY source;

--redaers/source 
 
SELECT source, COUNT (*)
FROM first_reader
GROUP BY source
ORDER BY source;

--TOPIC ANALYSIS
--revenue/continent

SELECT first_reader.topic, SUM (first_reader.rev_first) + SUM (returning_reader.rev_ret) AS result
FROM
(SELECT first_reader.topic, SUM (spent_reader.price) AS rev_first
FROM first_reader
JOIN spent_reader
ON first_reader.user_id = spent_reader.user_id
GROUP BY first_reader.topic) AS first_reader
JOIN
(SELECT returning_reader.topic, SUM (spent_reader.price) AS rev_ret
FROM returning_reader
JOIN spent_reader
ON returning_reader.user_id = spent_reader.user_id
GROUP BY returning_reader.topic) AS returning_reader
ON first_reader.topic = returning_reader.topic
GROUP BY first_reader.topic
ORDER BY first_reader.topic;

--readers/continent

SELECT first_reader.topic, SUM (first_reader.first_count) + SUM (returning_reader.returning_count)
FROM
(SELECT topic, COUNT (*) AS first_count
FROM first_reader
GROUP BY topic) AS first_reader
JOIN
(SELECT topic, COUNT (*) AS returning_count
FROM returning_reader
GROUP BY topic) AS returning_reader
ON first_reader.topic = returning_reader.topic
GROUP BY first_reader.topic
ORDER BY first_reader.topic;


--costumers/continent

SELECT first_reader.topic, SUM (first_count) + SUM (returning_count) AS result
FROM
  (SELECT topic, COUNT(*) AS first_count
  FROM first_reader
  RIGHT JOIN spent_reader
  ON first_reader.user_id = spent_reader.user_id
  GROUP BY topic) AS first_reader
JOIN
  (SELECT topic, COUNT(*) AS returning_count
  FROM returning_reader
  RIGHT JOIN spent_reader
  ON returning_reader.user_id = spent_reader.user_id
  GROUP BY topic) AS returning_reader
ON first_reader.topic = returning_reader.topic
GROUP BY first_reader.topic;

 --readers/costumers/continent visualization

SELECT first_reader.topic, SUM (first_reader.first_count) + SUM (returning_reader.returning_count) AS readers, SUM (first_reader_2.first_count) + SUM (returning_reader_2.returning_count)AS costumers
FROM
  (SELECT topic, COUNT (*) AS first_count
  FROM first_reader
  GROUP BY topic) AS first_reader
JOIN
  (SELECT topic, COUNT (*) AS returning_count
  FROM returning_reader
  GROUP BY topic) AS returning_reader
ON first_reader.topic = returning_reader.topic
JOIN
  (SELECT topic, COUNT(*) AS first_count
  FROM first_reader
  RIGHT JOIN spent_reader
  ON first_reader.user_id = spent_reader.user_id
  GROUP BY topic) AS first_reader_2
ON first_reader.topic = first_reader_2.topic
JOIN
  (SELECT topic, COUNT(*) AS returning_count
  FROM returning_reader
  RIGHT JOIN spent_reader
  ON returning_reader.user_id = spent_reader.user_id
  GROUP BY topic) AS returning_reader_2
ON first_reader.topic = returning_reader_2.topic
GROUP BY first_reader.topic;



--SEGMENTATION
--country/source/revenue

SELECT country, source, SUM (spent_reader.price)
FROM first_reader
JOIN spent_reader
ON first_reader.user_id = spent_reader.user_id
GROUP BY country, source
ORDER BY country;


---Daily active users

SELECT first_reader.date_event, SUM(first_count) + SUM(returning_count) AS active
FROM
  (SELECT date_event, COUNT(DISTINCT(user_id)) AS first_count
  FROM first_reader
  GROUP BY date_event) AS first_reader
JOIN
  (SELECT date_event, COUNT(DISTINCT(user_id)) AS returning_count
  FROM returning_reader
  GROUP BY date_event) AS returning_reader
ON first_reader.date_event = returning_reader.date_event
GROUP BY first_reader.date_event;

--Daily revenue

SELECT date_event, SUM (spent_reader.price)
FROM spent_reader
GROUP BY date_event
ORDER BY date_event;

---daily revenue from books 

SELECT date_event, SUM (spent_reader.price) AS book_revenue
FROM spent_reader
WHERE price = 8
GROUP BY date_event
ORDER BY date_event;

---daily revenue from video_courses

SELECT date_event, SUM (spent_reader.price) AS video_revenue
FROM spent_reader
WHERE price = 80
GROUP BY date_event
ORDER BY date_event;


--Daily revenue/book/video visualization

SELECT all_rev.date_event, revenue, book_revenue, video_revenue
FROM
  (SELECT date_event, SUM (spent_reader.price) AS revenue
  FROM spent_reader
  GROUP BY date_event
  ORDER BY date_event) AS all_rev
FULL JOIN
  (SELECT date_event, SUM (spent_reader.price) AS book_revenue
  FROM spent_reader
  WHERE price = 8
  GROUP BY date_event
  ORDER BY date_event) AS book
ON all_rev.date_event = book.date_event
FULL JOIN
  (SELECT date_event, SUM (spent_reader.price) AS video_revenue
  FROM spent_reader
  WHERE price = 80
  GROUP BY date_event
  ORDER BY date_event) AS video
ON book.date_event = video.date_event;


----country and source/readers(first,returning,subscribed, costumers) visualization

SELECT source, country,
COUNT(user_id) AS first_readers,
COUNT(user_id) FILTER (WHERE user_id IN (SELECT DISTINCT (user_id) FROM returning_reader)) AS returning_readers,
COUNT(user_id) FILTER (WHERE user_id IN (SELECT user_id FROM subscribed_reader)) AS subscribed_readers,
COUNT(user_id) FILTER (WHERE user_id IN (SELECT DISTINCT (user_id) FROM spent_reader)) AS spent_readers
FROM first_reader
GROUP BY source, country
ORDER BY source, country;
