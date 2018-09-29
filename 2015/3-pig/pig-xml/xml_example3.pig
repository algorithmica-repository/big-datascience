register /home/cloudera/piggybank-0.14.0.jar

DEFINE XmlLoader org.apache.pig.piggybank.storage.StreamingXMLLoader();
 
books = LOAD 'xml_input.xml'
            USING XmlLoader(
            'book',
            'title, author, country'
            ) AS (
                title:  {(attr:map[], content:chararray)},
                author: {(attr:map[], content:chararray)},
                country:   {(attr:map[], content:chararray)}
            );
 
dump books;
