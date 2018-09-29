package com.alg.pig.udfs;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;

public class UPPER extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			String str = (String) input.get(0);
			return str.toUpperCase();
		} catch (Exception e) {
			warn("Error", PigWarning.UDF_WARNING_1);
		}
		return null;
	}
}
