package com.itheima.mrdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMainTest extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //获取一个job对象，用于我们任务的组织.
        Job job = Job.getInstance(super.getConf(), "wordcount");

        job.setJarByClass(JobMainTest.class);

        //读取文件，解析成key,value对，这里是k1  v1
        job.setInputFormatClass(TextInputFormat.class);
        //这里使用本地模式来运行
        TextInputFormat.addInputPath(job,new Path("file:///C:\\Users\\JackyWuSueLee\\Downloads\\words02.txt"));

        //自定义map逻辑，接收第一步的k1,v1  转换成新的k2  v2  进行输出
        job.setMapperClass(WordCountTestMapper.class);
        //设置我们key2的类型
        job.setMapOutputKeyClass(Text.class);
        //设置我们的v2类型
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce类，接受我们的key2  v2  输出我们k3  v3
        job.setReducerClass(WordCountTestReducer.class);
        //设置我们key3输出的类型
        job.setOutputKeyClass(Text.class);
        //设置我们value3的输出类型
        job.setOutputValueClass(IntWritable.class);

        //设置输出类  outputformat
        job.setOutputFormatClass(TextOutputFormat.class);

        //使用本地模式来运行
        TextOutputFormat.setOutputPath(job,new Path("file:///C:\\Users\\JackyWuSueLee\\Downloads\\output2"));

        //提交我们的任务
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //提交我们的job任务
        //任务完成之后，返回一个状态码值，如果状态码值是0，表示程序运行成功
        int run = ToolRunner.run(configuration, new JobMainTest(), args);
        System.exit(run);
    }
}

