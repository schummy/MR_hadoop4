import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by user on 1/12/16.
 */
public class RowWritable implements Writable {
    private static final String EMPTY_STRING = new String("");

    private String textValue = EMPTY_STRING;
    private long partition = 0;
    private long counter = 0;
    protected Logger logger;
    private String delimiter = ",";

    public long getPartition() {
        return partition;
    }
    public long getCounter() {
        return counter;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getTextValue() {
        return textValue;
    }

    public void setPartition(long partition) {
        this.partition = partition;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }


    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public void setTextValue(String textValue) {
        this.textValue = textValue;
        counter = 0;
        partition=0;
    }


    public RowWritable(long partition, long count) {
        this.partition = partition;
        this.counter = count;
        textValue = EMPTY_STRING;
        setLogger(LoggerFactory.getLogger(RowWritable.class));
        //logger.info("RowWritable({}, {}) constructor call", sum, count);
    }

    public void setPartitionCounter(long partition, long counter) {
        this.counter = counter;
        this.partition = partition;
        this.textValue = EMPTY_STRING;
    }


    public RowWritable() {
        partition = 0;
        counter = 0;
        textValue = EMPTY_STRING;
        setLogger(LoggerFactory.getLogger(RowWritable.class));
        //logger.info("RowWritable constructor call");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        logger.info("RowWritable write call");
        if (textValue.isEmpty()) {
            dataOutput.writeLong(partition);
            dataOutput.writeLong(counter);
        }else
            dataOutput.writeChars(textValue);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        logger.info("RowWritable read call");
        textValue = dataInput.readLine();
        if (textValue.isEmpty()) {
            setPartition(dataInput.readLong());
            setCounter(dataInput.readLong());
        }
    }

    public String toString(){

        logger.info("RowWritable toString call");
        System.out.println("RowWritable toString call");

        return textValue.isEmpty()?(partition + delimiter + counter):textValue;
    }

    public int hashCode() {
        int result = 17;
        int constVal = 31;
        result = constVal * result + (int)(partition ^ (partition >>> 32));
        result = constVal * result + (int)(counter ^ (counter >>> 32));
        result = constVal * result + textValue.hashCode();
        return result;
    }
    public boolean equals(Object obj){
        if (obj == this)
            return true;
        if (!(obj instanceof RowWritable))
            return false;

        RowWritable rwObj = (RowWritable) obj;
        return rwObj.partition == partition && rwObj.counter==counter && textValue.equals(rwObj.textValue);
    }



}
