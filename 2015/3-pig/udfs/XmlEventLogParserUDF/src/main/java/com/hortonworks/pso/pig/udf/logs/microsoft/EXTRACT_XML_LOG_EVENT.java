package com.hortonworks.pso.pig.udf.logs.microsoft;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import java.io.IOException;
import java.io.StringReader;

/**
 * Using XPaths to extract fields from MS Logs (in XML representation).
 *
 * The resulting structure will be:
 * <pre>
 * {mslogevent:
 *      (eventlog: chararray,
 *       recordnumber: chararray,
 *       timegenerated: chararray,
 *       eventid: chararray,
 *       computername: chararray,
 *       eventtype: chararray,
 *       sourcename: chararray,
 *       eventcategory: chararray,
 *       eventtypename: chararray,
 *       eventcategoryname: chararray,
 *       strings:
 *              (f1: chararray,f2: chararray,f3: chararray,f4: chararray,f5: chararray,f6:chararray,
 *              f7: chararray,f8: chararray,f9: chararray,f10: chararray,f11: chararray,f12: chararray,
 *              f13: chararray,f14: chararray,f15: chararray,f16: chararray,f17: chararray,f18: chararray,
 *              f19: chararray,f20: chararray,f21: chararray,f22: chararray,f23: chararray,f24: chararray,
 *              f25: chararray),
 *       message: chararray)
 *       }
 * </pre>
 *
 * Source the process from a MS Event Log XML file with the format like:
 * <pre>
 *  &lt;?xml version="1.0" encoding="ISO-10646-UCS-2" standalone="yes" ?&gt;
    &lt;ROOT DATE_CREATED="2014-01-11 07:52:12" CREATED_BY="Microsoft Log Parser V2.2"&gt;
        &lt;ROW&gt;
         &lt;EventLog&gt;
         Security
         &lt;/EventLog&gt;
         &lt;RecordNumber&gt;
         31582791
         &lt;/RecordNumber&gt;
         &lt;TimeGenerated&gt;
         2014-01-11 07:22:09
         &lt;/TimeGenerated&gt;
         &lt;EventID&gt;
         4624
         &lt;/EventID&gt;
         &lt;ComputerName&gt;
         ...
         &lt;/ComputerName&gt;
         &lt;SID&gt;
         &lt;/SID&gt;
         &lt;EventType&gt;
         8
         &lt;/EventType&gt;
         &lt;SourceName&gt;
         Microsoft-Windows-Security-Auditing
         &lt;/SourceName&gt;
         &lt;EventCategory&gt;
         12544
         &lt;/EventCategory&gt;
         &lt;EventTypeName&gt;
         ...
         &lt;/EventTypeName&gt;
         &lt;EventCategoryName&gt;
         ...
         &lt;/EventCategoryName&gt;
         &lt;Strings&gt;
         ...|...|...|
         &lt;/Strings&gt;
         &lt;Message&gt;
         ...
         &lt;/Message&gt;
         &lt;Data&gt;
         &lt;/Data&gt;
         &lt;/ROW&gt;
    ....
    &lt;/ROOT&gt;
 * </pre>
 *
 * The "Strings" field is parsed and put into an array of tuples identified by f[1-25].  To access of filter on these fields use the following convention, as seen in this sample below. In this example, the data is sourced straight from the XML.
 *
 * <pre>
 register '/usr/lib/pig/piggybank.jar';
 register '/home/vagrant/hwx.pso/com.hortonworks.pso.pig.udf-1.0-SNAPSHOT-shaded.jar';

 DEFINE EXTRACT_XML_LOG_EVENT com.hortonworks.pso.pig.udf.logs.microsoft.EXTRACT_XML_LOG_EVENT;

 ROWS = load '$source' using org.apache.pig.piggybank.storage.XMLLoader('ROW')
    as (row:chararray);

 out = foreach ROWS generate EXTRACT_XML_LOG_EVENT(row);

 describe out;

 proj = foreach out generate
     mslogevent.recordnumber,
     mslogevent.eventid,
     mslogevent.computername,
     mslogevent.strings.f1;

 * </pre>
 *
 * Another approach is to 'convert' the XML logs and store in HDFS with PigStorage.  Here's the load process script:
 *
 * <pre>
    register '/usr/lib/pig/piggybank.jar';
    register '/home/vagrant/hwx.pso/com.hortonworks.pso.pig.udf-1.0-SNAPSHOT-shaded.jar';

    DEFINE EXTRACT_XML_LOG_EVENT com.hortonworks.pso.pig.udf.logs.microsoft.EXTRACT_XML_LOG_EVENT;

    A = load '$source' using org.apache.pig.piggybank.storage.XMLLoader('ROW')
        as (row:chararray);

    B = foreach A generate EXTRACT_XML_LOG_EVENT(row);

    STORE B INTO '$target' USING PigStorage();
 * </pre>
 *
 * And then a sample script to reload from above and use.
 *
 * <pre>
 A = load '$source' using PigStorage()
    as (mslogevent:
         (eventlog: chararray,
         recordnumber: chararray,
         timegenerated: chararray,
         eventid: chararray,
         computername: chararray,
         eventtype: chararray,
         sourcename: chararray,
         eventcategory: chararray,
         eventtypename: chararray,
         eventcategoryname: chararray,
         strings:
             (f1: chararray,f2: chararray,f3: chararray,f4: chararray,f5: chararray,
             f6: chararray,f7: chararray,f8: chararray,f9: chararray,f10: chararray,
             f11: chararray,f12: chararray,f13: chararray,f14: chararray,f15: chararray,
             f16: chararray,f17: chararray,f18: chararray,f19: chararray,f20: chararray,
             f21: chararray,f22: chararray,f23: chararray,f24: chararray,f25: chararray),
         message: chararray));

 describe A;

 B = filter A by mslogevent.eventid == '$eventid';

 C = foreach B generate
    mslogevent.recordnumber,
    mslogevent.eventid,
    mslogevent.computername,
    mslogevent.strings.f3;

 dump C;
 * </pre>
 *
 *
 */
public class EXTRACT_XML_LOG_EVENT extends EvalFunc<Tuple> {
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();

    private SAXBuilder builder = new SAXBuilder();
    private PathExpressionWrapper[] xpaths;
    private static final int STRINGS_FIELD_COUNT = 25;

    public static enum PATHS {
        EventLog(DataType.CHARARRAY),
        RecordNumber(DataType.CHARARRAY),
        TimeGenerated(DataType.CHARARRAY),
        EventID(DataType.CHARARRAY),
        ComputerName(DataType.CHARARRAY),
        EventType(DataType.CHARARRAY),
        SourceName(DataType.CHARARRAY),
        EventCategory(DataType.CHARARRAY),
        EventTypeName(DataType.CHARARRAY),
        EventCategoryName(DataType.CHARARRAY),
        Strings(DataType.TUPLE),
        Message(DataType.CHARARRAY);

        private byte dataType;

        public byte getDataType() {
            return dataType;
        }

        private PATHS(byte dataType) {
            this.dataType = dataType;
        }


    }

    private class PathExpressionWrapper {
        public XPathExpression<Element> xPathExpression;
        public PATHS paths;

        private PathExpressionWrapper(XPathExpression xPathExpression, PATHS paths) {
            this.xPathExpression = xPathExpression;
            this.paths = paths;
        }

    }

    public EXTRACT_XML_LOG_EVENT() {
        try {
            xpaths = createXpaths();

        } catch (JDOMException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to create UDF", e);
        }
    }

    public PathExpressionWrapper[] createXpaths() throws JDOMException {
        PathExpressionWrapper[] paths = new PathExpressionWrapper[PATHS.values().length];
        int idx = 0;
        for (PATHS path : PATHS.values()) {
            paths[idx++] =
                    new PathExpressionWrapper(XPathFactory.instance().compile("//" + path.toString(), Filters.element()), path);
        }
        return paths;
    }

    @Override
    public Tuple exec(Tuple objects) throws IOException {
        Tuple ret = tupleFactory.newTuple(xpaths.length);
        String xmlSnippet = (String) objects.get(0);
        Document doc = null;
        try {
            doc = builder.build(new StringReader(xmlSnippet));
        } catch (JDOMException e) {
            throw new RuntimeException("Unable to parse XML: " + xmlSnippet, e);
        }
        int idx = 0;
        for (PathExpressionWrapper pathWrapper : xpaths) {
            XPathExpression<Element> xpath = pathWrapper.xPathExpression;
            switch (pathWrapper.paths.dataType) {
                case DataType.BAG:
                    Element e = (Element) xpath.evaluateFirst(doc);
                    DataBag lclBag = bagFactory.newDefaultBag();
                    String content = e.getTextNormalize().trim();
                    String[] parts = content.split("\\|");
                    Tuple partTuple = tupleFactory.newTuple(parts.length);
                    int bagTupleIdx = 0;
                    for (String part: parts) {
                        partTuple.set(bagTupleIdx++, part);

                    }
                    lclBag.add(partTuple);
                    ret.set(idx++, lclBag);
                    break;
                case DataType.TUPLE:
                    Element et = (Element) xpath.evaluateFirst(doc);
                    String content1 = et.getTextNormalize().trim();
                    String[] parts1 = content1.split("\\|");
                    Tuple partTuple1 = tupleFactory.newTuple(STRINGS_FIELD_COUNT);
                    int tupleIdx = 0;
                    for (String part: parts1) {
                        // Only capture STRINGS_FIELD_COUNT fields.
                        if (tupleIdx >= STRINGS_FIELD_COUNT)
                            break;
                        partTuple1.set(tupleIdx++, part);
                    }
                    ret.set(idx++, partTuple1);
                    break;
                case DataType.CHARARRAY:
                    Element ec = (Element) xpath.evaluateFirst(doc);
                    ret.set(idx++, ec.getTextNormalize().trim());
                    break;
                default:
                    // TODO: More handling needed here. For now, we only have the two types used.
                    Element ed = (Element) xpath.evaluateFirst(doc);
                    ret.set(idx++, ed.getTextNormalize().trim());
                    break;
            }
        }
        return ret;
    }

    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            for (PATHS path : PATHS.values()) {
                switch (path.dataType) {
                    case DataType.CHARARRAY:
                        tupleSchema.add(new Schema.FieldSchema(path.toString().toLowerCase(), path.getDataType()));
                        break;
                    case DataType.BAG:
                        Schema bagSchema = new Schema();
                        Schema bagTupleSchema = new Schema();
                        for (int i = 1;i<=STRINGS_FIELD_COUNT;i++) {
                            bagTupleSchema.add(new Schema.FieldSchema("f"+i, DataType.CHARARRAY));
                        }
                        bagSchema.add(new Schema.FieldSchema("fields", bagTupleSchema));
                        tupleSchema.add(new Schema.FieldSchema(path.toString().toLowerCase(), bagSchema));
                        break;
                    case DataType.TUPLE:
                        Schema stringsTupleSchema = new Schema();
                        for (int i = 1;i<=STRINGS_FIELD_COUNT;i++) {
                            stringsTupleSchema.add(new Schema.FieldSchema("f"+i, DataType.CHARARRAY));
                        }
                        tupleSchema.add(new Schema.FieldSchema(path.toString().toLowerCase(), stringsTupleSchema));
                        break;
                }
            }
            return new Schema(new Schema.FieldSchema("mslogevent", tupleSchema));

        } catch (Exception e) {
            return null;
        }
    }
}
