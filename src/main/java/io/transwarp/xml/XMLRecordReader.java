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
 * 将XML解析为多个key value对以key=value的形式返回
 * Created by zxh on 2017/5/26.
 */
public class XMLRecordReader implements RecordReader<LongWritable, Text> {
    private static final Logger LOGGER = LoggerFactory.getLogger(XMLRecordReader.class);
    private static final String START_TAG_KEY = "xml.start.tag";
    private static final String END_TAG_KEY = "xml.end.tag";

    private FSDataInputStream fis;
    private byte[] startTag;
    private byte[] endTag;

    private long start;
    private long end;

    private DataOutputBuffer buffer;
    private SAXBuilder builder;
    private Document document;

    public XMLRecordReader(JobConf job, FileSplit split) throws IOException {
        startTag = job.get(START_TAG_KEY).getBytes("UTF-8");
        endTag = job.get(END_TAG_KEY).getBytes("UTF-8");

        start = split.getStart();
        end = start + split.getLength();

        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(job);
        fis = fs.open(path);
        fis.seek(start);

        buffer = new DataOutputBuffer();
        builder = new SAXBuilder();
    }

    /**
     * 遍历split获取value
     *
     * @param key
     * @param value
     * @return
     * @throws IOException
     */
    public boolean next(LongWritable key, Text value) throws IOException {
        if (fis.getPos() < end && readUnitlMatch(startTag, false)) {
            buffer.write(startTag);
            try {
                if (readUnitlMatch(endTag, true)) {
                    key.set(fis.getPos());
                    byte[] XMLbytes = new byte[buffer.getLength()];
                    //注意buffer.getData().length != buffer.getLength()
                    System.arraycopy(buffer.getData(), 0 , XMLbytes, 0 , buffer.getLength());
                    String kvStr = parseXML(XMLbytes);
                    value.set(kvStr);
                    LOGGER.info("XML parse:{}", kvStr);
                    return true;
                }
            } finally {
                buffer.reset();
            }

        }
        return false;
    }

    /**
     * 解析XML字符串为key=value形式，作为inputformat 的value返回
     * @param XML
     * @return
     */
    public String parseXML(byte[] XML){
        try {
            document = builder.build(new ByteArrayInputStream(XML));
        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(document == null)
            return null;
        List<Element> elements = document.getRootElement().getChildren();

        if (elements.get(0).getName().compareToIgnoreCase("name") == 0 &&
                elements.get(1).getName().compareToIgnoreCase("value") == 0) {
            return new StringBuilder().append(elements.get(0).getText())
                    .append("=")
                    .append(elements.get(1).getText())
                    .toString();
        }

        return null;
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
        return (fis.getPos() - start) / (float) (end - start);
    }

    /**
     * 解析XML文件为单个Eelement
     * @param tag
     * @param isWrite
     * @return
     * @throws IOException
     */
    public boolean readUnitlMatch(byte[] tag, boolean isWrite) throws IOException {
        int i = 0;
        while (true) {
            int b = fis.read();
            if (b == -1) {
                return false;
            }

            if (isWrite) {
                buffer.write(b);
            }

            if (b == tag[i]) {
                i++;
                if (i >= tag.length) {
                    return true;
                }
            } else {
                i = 0;
            }

            if (!isWrite && i == 0 && fis.getPos() >= end) {
                return false;
            }
        }
    }
}
