package com.bigdata.hadoop.testdata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import java.util.UUID;

public class DataGenerator {

	public static void createData(String file, long n) throws Exception {
		// String workingDir = System.getProperty("user.dir");
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(file)));
		Random r = new Random(100);
		bw.write("name,position,dept,salary");
		bw.newLine();
		for (long k = 0; k < n; k++) {
			String pos = "Position" + r.nextInt(1000);
			String dept = "Dept" + r.nextInt(1000);
			String name = UUID.randomUUID().toString();
			int sal = r.nextInt(100000) + 5000;
			bw.write(name + "," + pos + "," + dept + "," + sal);
			bw.newLine();
		}
		bw.close();
	}

	public static void main(String[] args) throws Exception {
		String file = args[0];
		int n = Integer.parseInt(args[1]);
		createData(file, n);
	}

}
