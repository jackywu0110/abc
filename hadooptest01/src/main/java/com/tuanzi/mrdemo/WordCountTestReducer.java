package com.tuanzi.mrdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountTestReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    /**
     * 覆写reduce方法，
     * @param key
     * @param values
     * @param context  上下文对象，将数据往外写
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       /*
       input is as below:
        a,b,c,e,y
        k,k,c,f,u
        a,b,c,d,o
        t,z,k,f,p
        t,a,j,u,w
        k,a,a,b,d

        As below is output:
        a	1
        c	3
        j	1
        k	1
        */
        int count = 0;
        for (IntWritable value : values) {
            int v = value.get();
            count += v;
        }
        //将数据写出去
        context.write(key,new IntWritable(count));
    }
}

