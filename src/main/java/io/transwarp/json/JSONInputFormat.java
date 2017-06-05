package io.transwarp.json;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * JSON InputFormat
 * Created by zxh on 2017/5/18.
 */
public class JSONInputFormat extends TextInputFormat implements JobConfigurable{
    private static final Logger LOGGER = LoggerFactory.getLogger(JSONInputFormat.class);
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(split.toString());
        return new JSONRecordReader(job, (FileSplit) split);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        FileStatus[] files = listStatus(job);

        LOGGER.info("status:{}",files.length);


        for(FileStatus file: files){
            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(job);

            BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());

            LOGGER.info("FILE:{},len:{}, rep:{}", file.getPath(), file.getLen(), file.getReplication());

            for(BlockLocation block:blocks){
                FileSplit split = new FileSplit(file.getPath(), 0, file.getLen(), block.getHosts());
                splits.add(split);
            }
        }

        job.setLong(NUM_INPUT_FILES, splits.size());
        LOGGER.info("total splits:{}", splits.size());
        return splits.toArray(new FileSplit[splits.size()]);
    }
}

