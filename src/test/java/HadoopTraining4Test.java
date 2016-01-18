import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by user on 1/13/16.
 */
public class HadoopTraining4Test {
    MapDriver<LongWritable, Text, Text, RowWritable> mapDriver;
    ReduceDriver<Text, RowWritable, Text, RowWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, RowWritable, Text, RowWritable> mapReduceDriver;

    @Before
    public void setUp() throws Exception {
        HadoopTraining4.HadoopTraining4Mapper mapper = new HadoopTraining4.HadoopTraining4Mapper();
        HadoopTraining4.HadoopTraining4Reducer reducer = new HadoopTraining4.HadoopTraining4Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapperNotZeroSize() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "ip1592 - - [28/Apr/2011:00:33:54 -0400] \"GET /sun_ss10/ss10_interior.jpg HTTP/1.1\" 200 12343 \"http://host2/sun_ss10/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0\""));
        mapDriver.withOutput(new Text("ip1592"), new RowWritable(12343, 1));
        mapDriver.runTest();
    }

    @Test
    public void testMapperZeroSize() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "ip1594 - - [28/Apr/2011:00:33:54 -0400] \"GET /sun_ss10/ss10_interior.jpg HTTP/1.1\" 200 - \"http://host2/sun_ss10/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0\""));
        mapDriver.withOutput(new Text("ip1594"), new RowWritable(0, 1));
        mapDriver.runTest();
    }

    @Test
    public void testMapperWrongSize() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "ip1594 - - [28/Apr/2011:00:33:54 -0400] \"GET /sun_ss10/ss10_interior.jpg HTTP/1.1\" 200 sdfsdfsd \"http://host2/sun_ss10/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0\""));
        mapDriver.withOutput(new Text("ip1594"), new RowWritable(0, 1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("ip1"), ImmutableList.of(new RowWritable(123, 3), new RowWritable(23, 4)));
        reduceDriver.withOutput(new Text("ip1"), new RowWritable(146, 7));
        reduceDriver.runTest();

    }

    @Test
    public void testMapReducer() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "ip1592 - - [28/Apr/2011:00:33:54 -0400] \"GET /sun_ss10/ss10_interior.jpg HTTP/1.1\" 200 12343 \"http://host2/sun_ss10/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0\""));
        mapReduceDriver.withOutput(new Text("ip1592"), new RowWritable(12343, 1));
        mapReduceDriver.runTest();
    }
}