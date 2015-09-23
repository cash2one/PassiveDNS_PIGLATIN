--------------------------------
-- META DESCRIPTION OF THE DATA 
--------------------------------
-- There are blank lines!!!
-- ~7 columns 

-- recordType variable can be the following: 
---- Q(Q) is the original query
---- Q(R) is the query echoed in the answer
---- R(ANS) is the answer
---- R(AUT) is authority
---- R(ADD) is additional

-- **Note: 1 Q(Q) => 1 Q(R) && one or many R(ANS)

-- Example:
-- 1434735481, Q(Q), c861aaa8307395e94c0bc1d88e9846ff168071252198640801b108219b3899be, IN, A, [Booter domain name]
-- 1434735481, Q(R), c861aaa8307395e94c0bc1d88e9846ff168071252198640801b108219b3899be, IN, A, [Booter domain name], NOERROR
-- 1434735481, R(ANS), c861aaa8307395e94c0bc1d88e9846ff168071252198640801b108219b3899be, IN, A, [Booter IP Address]

-------------------
-- LOADING CLASSES
-------------------
-- https://cwiki.apache.org/confluence/display/PIG/PiggyBank
REGISTER /usr/lib/pig/piggybank.jar ;
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(); 

-------------------
-- FEEDING THE PIG
-------------------
linesRaw = LOAD '../../data/2015*.gz' USING PigStorage(',') as (timestamp:int, recordType, srcIpAnon, alwaysIn , answerType, booterInformation, error);
/*linesRaw = LOAD '../dumps/anon_booters.txt' USING PigStorage(',') as (timestamp:long, recordType, srcIpAnon, alwaysIn , answerType, booterInformation, error);*/
lines = FILTER linesRaw BY timestamp is not null;
lines = FOREACH lines GENERATE 
					timestamp as timestamp, 
					-- remove prepended space from all values
                    REPLACE(recordType, ' ', '') as recordType,
                    REPLACE(srcIpAnon, ' ', '') as srcIpAnon,
                    REPLACE(alwaysIn, ' ', '') as alwaysIn, 
                    REPLACE(answerType, ' ', '') as answerType,
					-- normalize domain: lowercase and and remove www
                    REPLACE(LOWER(REPLACE(booterInformation, ' ', '')), 'www\\.', '') as booterInformation,
                    REPLACE(error, ' ', '') as error;

-------------------
-- 0. Deduplicate entries
-------------------
-- linesGroup = GROUP lines ALL; -- It groups all the lines
-- lines = FOREACH linesGroup {
-- 	b = lines.(timestamp, recordType, srcIpAnon, alwaysIn, answerType, booterInformation, error);
-- 	s = DISTINCT b;
-- 	GENERATE FLATTEN(s);
-- };
-- DUMP uniqLines;


-------------------
-- Counts the total number of lines in the group of lines
-------------------
numLines = FOREACH (GROUP lines ALL) GENERATE COUNT(lines);

-- -------------------
-- How many records is related to each recordType?
-- -------------------
numReqPerRecordType = FOREACH (group lines by recordType) GENERATE group as recordType, COUNT(lines);
-- STORE numReqPerRecordType INTO 'output/numReqPerRecordType' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-------------------
-- 3. How many requests (QQrecords) the database has?
-------------------
QQrecords = FILTER lines BY recordType == 'Q(Q)';
numQQrecords = FOREACH (GROUP QQrecords ALL) GENERATE COUNT(QQrecords);


-- #########################################################
-- ANALYSIS ON THE BOOTERS
-- #########################################################

-- ==========================================================
-- Which are the Booters requested && How many times each Booter was requested?
-- ==========================================================
numQQperBooter = FOREACH (GROUP QQrecords by booterInformation) GENERATE group as groupQQbyBooter, COUNT(QQrecords) as c;
sortedQQperBooter = ORDER numQQperBooter BY c DESC;
-- STORE sortedQQperBooter INTO 'output/QQperBooter'USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-- ==========================================================
-- Who are the srcIpAnon that request for a Booter AND How many request each srcIpAnon made?
-- ==========================================================
QQPerIp = FOREACH (GROUP  QQrecords by srcIpAnon) GENERATE group as groupIps, COUNT(QQrecords) as c;
sortedQQperIP = ORDER QQPerIp by c DESC;
-- STORE sortedQQperIP INTO 'output/sortedQQperIP' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-- ==========================================================
-- Time-series of the total number of requests X per day [time bin]
-- ==========================================================
%DECLARE onehour 3600
timeseries_queries_per_hour = FOREACH (GROUP QQrecords BY (timestamp/$onehour*$onehour)) GENERATE group, COUNT(QQrecords);
-- STORE timeseries_queries_per_hour INTO 'output/timeseries_queries_per_hour' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-- ==========================================================
%DECLARE oneDay 86400

group_sip = GROUP QQrecords BY srcIpAnon;

statistics_sip = FOREACH group_sip {
			timestamps_per_sip = FOREACH QQrecords GENERATE (timestamp/$oneDay*$oneDay) ;
			sum_of_timestamps_per_sip = COUNT(timestamps_per_sip);
			
			GENERATE group AS group_sip, timestamps_per_sip, sum_of_timestamps_per_sip;
	}
dump statistics_sip;

