package github.givensv;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TextInputFormat extends FileInputFormat<LongWritable, Text> {

    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context){

        return new LineRecordReader();

    }

    @Override
    public boolean isSplitable(JobContext context, Path file){

        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        if(null == codec) return true;
        return codec instanceof SplittableCompressionCodec;

    }

    public List<InputSplit> getSplit(JobContext job) throws IOException {

        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);

        List<InputSplit> splits = new ArrayList<InputSplit>();

        List<FileStatus> files = listStatus(job);

        for(FileStatus file : files){
            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(job.getConfiguration());
            long length = file.getLen();

            BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, length);
            if((length != 0) && isSplitable(job, path)){
                long blockSize = file.getBlockSize();
                long splitSize = computeSplitSize(blockSize, minSize, maxSize);
                long bytesRemaining = length;

                while( ((double)bytesRemaining / splitSize) > 1.1 ){

                }

            }
        }

    }

}
