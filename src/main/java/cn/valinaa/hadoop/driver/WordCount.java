package cn.valinaa.hadoop.driver;

import cn.valinaa.hadoop.mapper.WordCountMapper;
import cn.valinaa.hadoop.reducer.WordCountReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Valinaa
 * @Date : 2023/3/21
 * @Description : WordCount 程序示例
 */
public class WordCount {
    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException {
        // 启动job任务
        Job job = Job.getInstance();
        job.setJobName("WordCount");
        // 设置Mapper类、Reducer类
        job.setJarByClass(WordCount.class);  // 设置程序类
        job.setMapperClass(WordCountMapper.class);  // 设置Mapper类
        job.setReducerClass(WordCountReducer.class);  // 设置Reducer类
    
        // 设置Job输出结果<key,value>的中key和value数据类型，因为结果是<单词,个数>，所以key设置为"Text"类型，Value设置为"IntWritable"类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    
        // hdfs文件系统
        Path in = new Path("hdfs://localhost:9000/mr_demo/input/cust_fav"); //需要统计的文本所在位置
        Path out = new Path("hdfs://localhost:9000/mr_demo/output");  // 输出文件夹不能存在
    
        // 本地文件系统。若文件在本地文件系统，则替换为以下代码
        // Path in = new Path("file:///usr/local/java/data/mapreduce_demo/input/data_click"); // 用本地文件输入
        // Path out = new Path("file:///usr/local/java/data/mapreduce_demo/output"); // 结果输出到本地，文件夹不能已经存在
    
        // 设置job执行作业时输入和输出文件的路径
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
    
        // 无论程序是否执行成功，均强制退出
        // 如果程序成功运行，返回true，则程序返回0；如果程序执行失败，返回false，则程序返回1
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
