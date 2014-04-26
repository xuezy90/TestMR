package com.xinghuan.test;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xue on 4/26/14.
 */
public class XueWritable implements Writable{
    public XueWritable()
    {

    }
    private long timestamp = 0l;
    private float value = 0.0f;
    public XueWritable(long ts,float value)
    {
        this.timestamp = ts;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public float getValue() {
        return value;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setValue(float value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeFloat(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.timestamp = in.readLong();
        this.value = in.readFloat();
    }


}
