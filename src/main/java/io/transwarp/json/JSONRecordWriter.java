package io.transwarp.json;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by zxh on 2017/5/22.
 */
public class JSONRecordWriter implements RecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(JSONRecordWriter.class);

    private FSDataOutputStream out;

    public JSONRecordWriter(FSDataOutputStream out){
        this.out = out;
    }

    public void write(Writable writable) throws IOException {
        Text text = (Text)writable;
        String str = text.toString();
        out.write(str.getBytes(), 0, str.length());
    }

    public void close(boolean b) throws IOException {
        out.flush();
        out.close();
    }
}
