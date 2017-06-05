package io.transwarp.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by zxh on 2017/6/5.
 */
public class JSONRecordReader implements RecordReader<LongWritable, Text> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JSONRecordReader.class);

    private FSDataInputStream fis;
    private byte[] JSONBytes;

    private long start;
    private long end;
    private long length;




    public JSONRecordReader(JobConf job, FileSplit split) {
        Path path = split.getPath();

        length = split.getLength();
        start = split.getStart();
        end = start + length;

        JSONBytes = new byte[(int) length];

        LOGGER.info("split path:{}, start:{}, end:{}", path.toString(), start, end);

        try {
            FileSystem fs = path.getFileSystem(job);
            fis = fs.open(path);
            fis.seek(start);
        } catch (IOException e) {
            LOGGER.error("Error in open split, file:{},msg:{}",path.getName(), e.getMessage());
        }
    }

    public boolean next(LongWritable key, Text value) throws IOException {
        String json = parseJSON();
        if(json == null){
            return false;
        }

        key.set(key.get() + 1);
        value.set(json);
        return true;
    }

    private String parseJSON(){
        try {
            if(fis.getPos()>=end)
                return null;

            if(fis.read(JSONBytes, 0, (int) length) != length){
                LOGGER.error("read bytes less than the length of split");
                return null;
            }

            String jsonStr = new String(JSONBytes, 0, (int) length, "UTF-8");
            LOGGER.info("JSON:{}",jsonStr);


            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> json = mapper.readValue(jsonStr, Map.class);

            String line = mapper.writeValueAsString(json);
            LOGGER.info("line:{}", line);
            return line;
        } catch (IOException e) {
            LOGGER.error("Error in parse JSON, msg:{}", e.getMessage());
        }
        return null;
    }

    public LongWritable createKey() {
        return new LongWritable(0);
    }


    public Text createValue() {
        return new Text("");
    }

    public long getPos() throws IOException {
        return fis.getPos();
    }

    public void close() throws IOException {
        if(null != fis)
            fis.close();
    }

    public float getProgress() throws IOException {
        return (fis.getPos() - start)/(float)length;
    }
}
