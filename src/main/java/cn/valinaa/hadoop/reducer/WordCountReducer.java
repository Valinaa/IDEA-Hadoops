package cn.valinaa.hadoop.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Valinaa
 * @Date : 2023/3/21
 * @Description : [描述信息]
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private final IntWritable result=new IntWritable();
    @Override
    protected void reduce(Text key,Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
        int sum=0;
        for(IntWritable value : values){
            sum+=value.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
