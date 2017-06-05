package io.transwarp.json;

import com.sun.corba.se.spi.ior.Writeable;
import io.transwarp.xml.XMLRecordWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;


/**
 * Created by zxh on 2017/5/22.
 */
public class JSONOutputFormat<K extends LongWritable, V extends  Writeable>
        extends TextOutputFormat<K,V>
        implements HiveOutputFormat<K,V>{

    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf,
                                                             Path path,
                                                             Class<? extends Writable> aClass,
                                                             boolean b,
                                                             Properties properties,
                                                             Progressable progressable) throws IOException {
        FileSystem fs = path.getFileSystem(jobConf);
        FSDataOutputStream out = fs.create(path);
        return new JSONRecordWriter(out);
    }
}
