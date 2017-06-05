package io.transwarp.xml;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

/**
 * 将一个XML文件解析为value字符串，以value1,value2,value3....的形式返回
 * Created by zxh on 2017/5/26.
 */
public class XMLRecordReader2 implements RecordReader<LongWritable, Text> {
    private static final Logger LOGGER = LoggerFactory.getLogger(XMLRecordReader2.class);
    private static final String PROP_NUM = "xml.property.num";

    private FSDataInputStream fis;
    private byte[] XMLbytes;

    private long start;
    private long end;

    private DataOutputBuffer buffer;
    private SAXBuilder builder;
    private Document document;
    private StringBuilder valueBuilder;

    public XMLRecordReader2(JobConf job, FileSplit split) throws IOException {
        start = split.getStart();
        end = start + split.getLength();

        Path path = split.getPath();
        LOGGER.info("split path:{}, start:{}, end:{}", path.toString(), start, end);
        XMLbytes = new byte[(int) split.getLength()];

        FileSystem fs = path.getFileSystem(job);
        fis = fs.open(path);
        fis.seek(start);

        buffer = new DataOutputBuffer();
        builder = new SAXBuilder();
        valueBuilder = new StringBuilder();
    }


    /**
     * 获取key value
     *
     * @param key
     * @param value
     * @return
     * @throws IOException
     */
    public boolean next(LongWritable key, Text value) throws IOException {
        String v = parseXML();
        if(v == null)
            return false;
        key.set(key.get() + 1);
        value.set(v);
        return true;
    }

    /**
     * 解析XML字符串为value1,value2...形式
     *
     * @return
     */
    public String parseXML() throws IOException {
        if (fis.getPos() >= end)
            return null;
        try {
            fis.read(XMLbytes);
        } catch (IOException e) {
            LOGGER.error("error in reading bytes, msg:{}", e.getMessage());
            e.printStackTrace();
        }
        try {
            document = builder.build(new ByteArrayInputStream(XMLbytes));
        } catch (JDOMException e) {
            LOGGER.info("XML error, msg:{}", e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            LOGGER.info("XML error, msg:{}", e.getMessage());
            e.printStackTrace();
        }

        StringBuilder builder = new StringBuilder();
        List<Element> properties = document.getRootElement().getChildren();

        for (Element e : properties) {
            builder.append(e.getChild("value").getText()).append("\001");
        }

        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }


    public LongWritable createKey() {
        return new LongWritable(0);
    }

    public Text createValue() {
        return new Text("");
    }

    /**
     * 返回当前处理位置
     *
     * @return
     * @throws IOException
     */
    public long getPos() throws IOException {
        return fis.getPos();
    }

    public void close() throws IOException {
        if (fis == null) {
            fis.close();
        }
    }

    /**
     * 返回当前split处理进度
     *
     * @return
     * @throws IOException
     */
    public float getProgress() throws IOException {
        float progress = (fis.getPos() - start) / (float) (end - start);
        LOGGER.info("progress:{}, pos:{}", progress, fis.getPos());
        return progress;
    }

}
