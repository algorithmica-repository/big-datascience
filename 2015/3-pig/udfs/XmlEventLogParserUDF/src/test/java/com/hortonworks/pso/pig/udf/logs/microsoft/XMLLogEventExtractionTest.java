package com.hortonworks.pso.pig.udf.logs.microsoft;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by cstella on 3/21/14.
 */
public class XMLLogEventExtractionTest {
    @Test
    public void testExtractionUDF_Integration() throws IOException, ParseException {
        String[] rows = new String[] {"<ROW>" +
                "  <EventLog>" +
                "  Security" +
                "  </EventLog>" +
                "  <RecordNumber>" +
                "  31582791" +
                "  </RecordNumber>" +
                "  <TimeGenerated>" +
                "  2014-01-11 07:22:09" +
                "  </TimeGenerated>" +
                "  <EventID>" +
                "  4624" +
                "  </EventID>" +
                "  <ComputerName>" +
                "  MYWIN8SRVR1.mycorp.com" +
                "  </ComputerName>" +
                "  <SID>" +
                "  </SID>" +
                "  <EventType>" +
                "  8" +
                "  </EventType>" +
                "  <SourceName>" +
                "  Microsoft-Windows-Security-Auditing" +
                "  </SourceName>" +
                "  <EventCategory>" +
                "  12544" +
                "  </EventCategory>" +
                "  <EventTypeName>" +
                "  Success Audit event" +
                "  </EventTypeName>" +
                "  <EventCategoryName>" +
                "  The name for category 12544 in Source \"Microsoft-Windows-Security-Auditing\" cannot be found. The local computer may not have the necessary registry information or message DLL files to display messages from a remote computer" +
                "  </EventCategoryName>" +
                "  <Strings>" +
                "  S-1-0-0|-|-|0x0|S-1-5-21-329068152-1326574676-725345543-244806|MC36ORIONPOLLER$|TESTDMN|0xc6e99448|3|Kerberos|Kerberos||{C5B7B67F-2E92-3161-D44A-558A943055DB}|-|-|0|0x0|-|10.0.4.32|36435" +
                "  </Strings>" +
                "  <Message>" +
                "  An account was successfully logged on. Subject: Security ID: S-1-0-0 Account Name: - Account Domain: - Logon ID: 0x0 Logon Type: 3 New Logon: Security ID: S-1-5-21-329068152-1326574676-725345543-244806 Account Name: MC36ORIONPOLLER$ Account Domain: TESTDMN Logon ID: 0xc6e99448 Logon GUID: {C5B7B67F-2E92-3161-D44A-558A943055DB} Process Information: Process ID: 0x0 Process Name: - Network Information: Workstation Name: Source Network Address: 10.0.4.32 Source Port: 36435 Detailed Authentication Information: Logon Process: Kerberos Authentication Package: Kerberos Transited Services: - Package Name (NTLM only): - Key Length: 0 This event is generated when a logon session is created. It is generated on the computer that was accessed. The subject fields indicate the account on the local system which requested the logon. This is most commonly a service such as the Server service, or a local process such as Winlogon.exe or Services.exe. The logon type field indicates the kind of logon that occurred. The most common types are 2 (interactive) and 3 (network). The New Logon fields indicate the account for whom the new logon was created, i.e. the account that was logged on. The network fields indicate where a remote logon request originated. Workstation name is not always available and may be left blank in some cases. The authentication information fields provide detailed information about this specific logon request. - Logon GUID is a unique identifier that can be used to correlate this event with a KDC event. - Transited services indicate which intermediate services have participated in this logon request. - Package name indicates which sub-protocol was used among the NTLM protocols. - Key length indicates the length of the generated session key. This will be 0 if no session key was requested." +
                "  </Message>" +
                "  <Data>" +
                "  </Data>" +
                " </ROW>"
        };
        /*
         * Let's set some properties to ensure that we don't blow up with OOMs..because, you know, dealing with a few K should take >
         */
        System.getProperties().setProperty("mapred.map.child.java.opts", "-Xmx1G");
        System.getProperties().setProperty("mapred.reduce.child.java.opts","-Xmx1G");
        System.getProperties().setProperty("io.sort.mb","10");
        PigTest test = new PigTest( "src/main/pig/mslogevents/Driver.pig", new String[] {"input=dummy", "output=dummy"} );
        //System.out.println(rows[0]);
        String[] expectedOutput = new String[] {"((Security,31582791,2014-01-11 07:22:09,4624,MYWIN8SRVR1.mycorp.com,8,Microsoft-Windows-Security-Auditing,12544,Success Audit event,The name for category 12544 in Source \"Microsoft-Windows-Security-Auditing\" cannot be found. The local computer may not have the necessary registry information or message DLL files to display messages from a remote computer,(S-1-0-0,-,-,0x0,S-1-5-21-329068152-1326574676-725345543-244806,MC36ORIONPOLLER$,TESTDMN,0xc6e99448,3,Kerberos,Kerberos,,{C5B7B67F-2E92-3161-D44A-558A943055DB},-,-,0,0x0,-,10.0.4.32,36435,,,,,),An account was successfully logged on. Subject: Security ID: S-1-0-0 Account Name: - Account Domain: - Logon ID: 0x0 Logon Type: 3 New Logon: Security ID: S-1-5-21-329068152-1326574676-725345543-244806 Account Name: MC36ORIONPOLLER$ Account Domain: TESTDMN Logon ID: 0xc6e99448 Logon GUID: {C5B7B67F-2E92-3161-D44A-558A943055DB} Process Information: Process ID: 0x0 Process Name: - Network Information: Workstation Name: Source Network Address: 10.0.4.32 Source Port: 36435 Detailed Authentication Information: Logon Process: Kerberos Authentication Package: Kerberos Transited Services: - Package Name (NTLM only): - Key Length: 0 This event is generated when a logon session is created. It is generated on the computer that was accessed. The subject fields indicate the account on the local system which requested the logon. This is most commonly a service such as the Server service, or a local process such as Winlogon.exe or Services.exe. The logon type field indicates the kind of logon that occurred. The most common types are 2 (interactive) and 3 (network). The New Logon fields indicate the account for whom the new logon was created, i.e. the account that was logged on. The network fields indicate where a remote logon request originated. Workstation name is not always available and may be left blank in some cases. The authentication information fields provide detailed information about this specific logon request. - Logon GUID is a unique identifier that can be used to correlate this event with a KDC event. - Transited services indicate which intermediate services have participated in this logon request. - Package name indicates which sub-protocol was used among the NTLM protocols. - Key length indicates the length of the generated session key. This will be 0 if no session key was requested.))"
        };
        test.assertOutput("ROWS", rows, "out", expectedOutput);
    }
}
