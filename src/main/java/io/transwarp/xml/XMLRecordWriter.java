package io.transwarp.xml;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by zxh on 2017/5/22.
 */
public class XMLRecordWriter implements RecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(XMLRecordWriter.class);
    private static final String FIELD_SEP = "=";

    private FSDataOutputStream out;

    public XMLRecordWriter(FSDataOutputStream out){
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
