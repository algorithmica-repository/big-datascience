DEFINE EXTRACT_XML_LOG_EVENT com.hortonworks.pso.pig.udf.logs.microsoft.EXTRACT_XML_LOG_EVENT;

ROWS = LOAD '$input' USING PigStorage() as (row:chararray);
out = foreach ROWS generate EXTRACT_XML_LOG_EVENT(row);

describe out;

--STORE out into '$output' using PigStorage();
