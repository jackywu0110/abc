package com.itheima.mrdemo;

import com.itheima.mrbean.BloombergTradeBean;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLBloombergdataMapper  extends Mapper<LongWritable,Text,Text,NullWritable> {
    Text text = new Text();
    NullWritable nv = NullWritable.get();
    BloombergTradeBean bloombergTradeBean = new BloombergTradeBean();

    /**
     *
     * @param key  我们的key1   行偏移量 ，一般没啥用，直接可以丢掉
     * @param value  我们的value1   行文本内容，需要切割，然后转换成新的k2  v2  输出
     * @param context  上下文对象，承接上文，把数据传输给下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String str_header = "TradeId,Status,Library,AsOfDate,MarketDate,TradeQuery,PV,Currency";
        String[] split = line.split(",");
        if (line.equals(str_header)){
            return;
        }else if (split.length == 8 && !split[6].equals("null")){
            bloombergTradeBean.setTradeId(split[0]);
            bloombergTradeBean.setStatus(split[1]);
            bloombergTradeBean.setLibrary(split[2]);
            bloombergTradeBean.setAsOfDate(split[3]);
            bloombergTradeBean.setMarketDate(split[4]);
            bloombergTradeBean.setTradeQuery(split[5]);
            bloombergTradeBean.setCurrency(split[7]);
            System.out.println(split[6]);
            bloombergTradeBean.setPv(new Double(split[6]));
            text.set(bloombergTradeBean.toString());
            context.write(text,nv);
        }
    }
}

