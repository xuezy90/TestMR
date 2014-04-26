package com.xinghuan.test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xue on 4/26/14.
 */
public class XueIputFormat extends FileInputFormat<NullWritable,XueWritable> {

    @Override
    public RecordReader<NullWritable, XueWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        return new XueRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    class XueRecordReader extends RecordReader<NullWritable,XueWritable>
    {
        private DataOutput out ;
        private NullWritable key;
        private XueWritable value;
        public XueRecordReader()
        {
            key = new NullWritable();
        }
        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return false;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return null;
        }

        @Override
        public XueWritable getCurrentValue() throws IOException, InterruptedException {
            return null;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
