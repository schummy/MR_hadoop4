import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Created by user on 1/12/16.
 */
public class HadoopTraining4 extends Configured implements Tool {
    private Logger logger;
    private String outputSeparator = ",";
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    @Override
    public int run(String[] strings) throws Exception {
        setLogger(LoggerFactory.getLogger(HadoopTraining4.class));
        Configuration conf = getConf();


        Job job = Job.getInstance(conf, "Hadoop Training 4");
       // setTextoutputformatSeparator(job, outputSeparator);
       // setCompressionProperties(job);

        job.setNumReduceTasks(0);

        job.setJarByClass(HadoopTraining4Mapper.class);
        job.setMapperClass(HadoopTraining4Mapper.class);
      //  job.setCombinerClass(IPCombiner.class);
        job.setReducerClass(HadoopTraining4Reducer.class);

        // map output types
        job.setMapOutputKeyClass(ByteWritable.class);
        job.setMapOutputValueClass(RowWritable.class);

        // reducer output types
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1] +
                //DateTimeFormatter.ofPattern("_yyyyMMddHHmmss").format(LocalDateTime.now()).toString()));
                System.currentTimeMillis() ) );
        SequenceFileOutputFormat.setOutputPath(job, new Path(strings[1] +
                //DateTimeFormatter.ofPattern("_yyyyMMddHHmmss").format(LocalDateTime.now()).toString()));
                System.currentTimeMillis() ) );
        int result = job.waitForCompletion(true)? 0 : 1;
        for(Counter cnt: job.getCounters().getGroup("Browser"))
        {
            System.out.println(cnt.getName() + ":" + cnt.getValue());
        }

        return result;
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HadoopTraining4(), args);
        System.exit(res);

    }

    protected static void setTextoutputformatSeparator(final Job job, final String separator){
        final Configuration conf = job.getConfiguration(); //ensure accurate config ref

        conf.set("mapred.textoutputformat.separator", separator); //Prior to Hadoop 2 (YARN)
        conf.set("mapreduce.textoutputformat.separator", separator);  //Hadoop v2+ (YARN)
        conf.set("mapreduce.output.textoutputformat.separator", separator);
        conf.set("mapreduce.output.key.field.separator", separator);
        conf.set("mapred.textoutputformat.separatorText", separator); // ?
        conf.set("mapreduce.textoutputformat.separator", separator);  //Hadoop v2+ (YARN)


    }
    protected static void setCompressionProperties(final Job job) {
        final Configuration conf = job.getConfiguration(); //ensure accurate config ref

        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.SnappyCodec");
    }

    public static class HadoopTraining4Mapper extends Mapper<LongWritable, Text, ByteWritable, RowWritable>{

        private RowWritable value = new RowWritable();
        private Logger logger;
        private long counters[];

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            int numReduceTasks = 3;
            counters = new long[numReduceTasks];
        }

        @Override
        protected void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            logger = LoggerFactory.getLogger(HadoopTraining4Mapper.class);

            //logger.info("map started");

            Pattern p = Pattern.compile(logEntryPattern);
            Matcher matcher = p.matcher(value.toString());
            if (!matcher.matches() ||
                    NUM_FIELDS != matcher.groupCount()) {
                logger.warn("Bad log entry:\n {}", value.toString());
                return;
            }
            ip.set(matcher.group(1));

            if (StringUtils.isNumeric( matcher.group(7)) ) {
                sumCount.setSum(Integer.parseInt(matcher.group(7)));
                context.write(ip, sumCount);
            } else {
                context.write(ip, zeroSize);
            }


            //context.write(ip, intWritable);



        }
    }
    public static class HadoopTraining4Reducer extends Reducer<ByteWritable, RowWritable, LongWritable, Text> {
        private static LongWritable outputKey = new LongWritable();
        private static Text outputValue = new Text();
        @Override
        protected void reduce(ByteWritable key, Iterable<RowWritable> values, Context context) throws IOException, InterruptedException {
           //  logger.info("Reducer reduce started");
            //for (RowWritable val:values) {
            //}
            context.write(outputKey, outputValue);
            //context.write(key, new Text(sum + ", " + count));

        }
    }


}
