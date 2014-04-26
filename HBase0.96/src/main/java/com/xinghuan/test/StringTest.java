package com.xinghuan.test;

import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.StringTokenizer;

public class StringTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		DataOutput out = new DataOutputStream(new FileOutputStream(new File("/home/xue/xuezhongya/hadoop-dir/wordcount/input/test/xuewritable")));
		for(int i=0;i<10000;i++)
		{
            Writable writ = new XueWritable(System.currentTimeMillis(), (float)(Math.random()*100));
            writ.write(out);
		}
	}

}
