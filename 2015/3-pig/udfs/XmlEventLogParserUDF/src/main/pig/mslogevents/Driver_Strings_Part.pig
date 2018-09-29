DEFINE EXTRACT_XML_LOG_EVENT com.hortonworks.pso.pig.udf.logs.microsoft.EXTRACT_XML_LOG_EVENT;

ROWS = LOAD '$input' USING PigStorage() as (row:chararray);
out = foreach ROWS generate EXTRACT_XML_LOG_EVENT(row);
describe out;

proj = foreach out generate
	mslogevent.recordnumber,
	mslogevent.eventid,
	mslogevent.computername,
	mslogevent.strings.f1;

--STORE proj into '$output' using PigStorage();
