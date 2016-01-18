import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RowNumberJob extends Configured implements Tool {
    public final static byte COUNTER_MARKER = (byte) 'A';
    public final static byte VALUE_MARKER = (byte) 'Z';

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RowNumberJob(), args);
        System.exit(res);

    }
    public int run(String[] strings) {
        try {
            Configuration conf = getConf();
            Job job = Job.getInstance(conf, "Row number generator job.");
            job.setGroupingComparatorClass(IndifferentComparator.class);
            job.setPartitionerClass(Partitioner.class);
            job.setNumReduceTasks(0);


            job.setMapperClass(RowNumberMapper.class);
            job.setMapOutputKeyClass(ByteWritable.class);
            job.setMapOutputValueClass(RowNumberWritable.class);

            job.setReducerClass(RowNumberReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(strings[1] +
                    new SimpleDateFormat("_yyyyMMddHHmmss").format(new Date()) )
                    );


            job.setJarByClass(RowNumberJob.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(strings[0]));

                     job.waitForCompletion(true);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
            return 1;
        }

        return 0;
    }

    static class RowNumberMapper extends Mapper<LongWritable, Text, ByteWritable, RowNumberWritable> {
        private long[] counters;
        private int numReduceTasks;

        private RowNumberWritable outputValue = new RowNumberWritable();
        private ByteWritable outputKey = new ByteWritable();

        protected void setup(Context context) throws IOException, InterruptedException {
            numReduceTasks = 3;//context.getNumReduceTasks();
            counters = new long[numReduceTasks];
            outputKey.set(VALUE_MARKER);
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            outputValue.setValue(value);
            context.write(outputKey, outputValue);
            counters[Partitioner.partitionForValue(outputValue, numReduceTasks)]++;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputKey.set(COUNTER_MARKER);
            for(int c = 0; c < counters.length - 1; c++) {
                if (counters[c] > 0) {
                    outputValue.setCounter(c + 1, counters[c]);
                    context.write(outputKey, outputValue);
                }
                counters[c + 1] += counters[c];
            }
        }
    }

    static class RowNumberReducer extends Reducer<ByteWritable, RowNumberWritable, Text, Text> {
        private Text outputKey = new Text();

        protected void setup(Context context) throws IOException, InterruptedException {

        }

        protected void reduce(ByteWritable key, Iterable<RowNumberWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<RowNumberWritable> itr = values.iterator();
            if (!itr.hasNext()) {
                return;
            }

            long offset = 0;
            RowNumberWritable value = itr.next();
            while (itr.hasNext() && value.getCount() > 0) {
                offset += value.getCount();
                value = itr.next();
            }
            outputKey.set(Long.toString(offset++));
            context.write(outputKey, value.getValue());

            while(itr.hasNext()) {
                value = itr.next();
                outputKey.set(Long.toString(offset++));
                context.write(outputKey, value.getValue());
            }
        }
    }

    public static class IndifferentComparator implements RawComparator<ByteWritable> {
        @Override
        public int compare(ByteWritable left, ByteWritable right) {
            return 0;
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return 0;
        }
    }
    public static class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<ByteWritable, RowNumberWritable> {
        @Override
        public int getPartition(ByteWritable key, RowNumberWritable value, int numPartitions) {
            if (key.get() == (byte) RowNumberJob.COUNTER_MARKER) {
                return value.getPartition();
            } else {
                return Partitioner.partitionForValue(value, numPartitions);
            }
        }

        public static int partitionForValue(RowNumberWritable value, int numPartitions) {
            return (value.getValue().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}