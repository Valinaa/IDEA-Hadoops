package cn.valinaa.hadoop.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 字数映射器
 *
 * @author Valinaa
 * @Description : Mapreduce 示例代码
 * @date 2023/03/21
 */

public class WordCountMapper extends Mapper<Object, Text,Text, IntWritable> {
    public static final IntWritable ONE =new IntWritable(1);
    @Override
    protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
        String[] splits=value.toString().split("\t");
        for (String word : splits){
            context.write(new Text(word),ONE);
        }
    }
}
