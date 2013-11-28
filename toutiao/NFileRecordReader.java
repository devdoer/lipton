package toutiao;

import java.io.IOException;
//import toutiao.FileLineWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
//import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.util.LineReader;


public class NFileRecordReader implements RecordReader<Text, Text>{
    private long start;
    private long end;
    private long pos;
    private LineReader reader;
    private Path path;

    public NFileRecordReader(CombineFileSplit split, Configuration conf, Reporter reporter, Integer index) throws IOException{
        this.path = split.getPath(index);
        FileSystem fs = this.path.getFileSystem(conf);
        this.start = split.getOffset(index);
        this.pos = start;
        this.end = start + split.getLength(index);

        FSDataInputStream fileIn = fs.open(path);
        reader = new LineReader(fileIn);
    }


    @Override
        public void close() throws IOException {
                if(reader!=null)
                    reader.close();
            }
    @Override
        public long getPos() throws IOException{
            return this.pos;
        }

    @Override
        public float getProgress() throws IOException{
            if (start == end) {
                return 0;
            }
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }

    @Override
        public Text createKey(){
            return new Text();
        }

    @Override
        public Text createValue(){
            return new Text();    
        }
        



    @Override
        public boolean next(Text key, Text value) throws IOException{
            //key.set( pos );
            int newSize = 0;
            if (pos < end) {
                newSize = reader.readLine(value);
                pos += newSize;
            }
            if (newSize == 0) {
                //key = null;
                //value = null;
                return false;
            } else{
                return true;
            }
        }
}
