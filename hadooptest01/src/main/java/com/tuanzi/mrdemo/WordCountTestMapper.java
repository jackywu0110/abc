package com.tuanzi.mrdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountTestMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    Text text = new Text();
    IntWritable intWritable  =  new IntWritable();

    /**
     * 覆写父类的map方法，每一行数据要调用一次map方法，我们的处理逻辑都写在这个map方法里面
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       /*
        a,b,c,e,y
        k,k,c,f,u
        a,b,c,d,o
        t,z,k,f,p
        j,j,j,j,w
        k,a,a,b,d
        */
        String line = value.toString();
        String[] splitted_list = line.split(",");
        text.set(splitted_list[2]);    //统计的是第3列的字母。
        intWritable.set(1);
        context.write(text,intWritable);
    }
}

