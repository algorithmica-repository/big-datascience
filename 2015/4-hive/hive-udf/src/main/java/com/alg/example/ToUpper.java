package com.matthewrathbone.example;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


@Description(
		name = "toupper",
		value = "_FUNC_(str) - Converts a string to uppercase",
		extended = "Example:\n" +
		"  > SELECT toupper(author_name) FROM authors a;\n" +
		"  STEPHEN KING"
		)
	public class ToUpper extends UDF {

	    public Text evaluate(Text s) {
			Text to_value = new Text("");
			if (s != null) {
			    try { 
					to_value.set(s.toString().toUpperCase());
			    } catch (Exception e) { 
					to_value = new Text(s);
			    }
			}
			return to_value;
	    }
	}

//add jar /home/cloudera/.jar
//create temporary function to_upper as 'com.alg.hive.ToUpper';
//SELECT toupper(author_name) FROM authors a;