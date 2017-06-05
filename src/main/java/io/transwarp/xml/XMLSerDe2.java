package io.transwarp.xml;

/**
 * Created by zxh on 2017/5/22.
 */

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class XMLSerDe2 extends AbstractSerDe {
    private static final Logger LOGGER = LoggerFactory.getLogger(XMLSerDe2.class);
    private static final String FIELD_SEP = "\001";
    private List<String> columnNames = null;
    private List<TypeInfo> columnTypes = null;
    private ObjectInspector objectInspector = null;

    public void initialize(Configuration configuration, Properties properties) throws SerDeException {
        //read column info
        String colNameStr = properties.getProperty(Constants.LIST_COLUMNS);
        columnNames = Arrays.asList(colNameStr.split(","));

        String colTypeStr = properties.getProperty(Constants.LIST_COLUMN_TYPES);
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypeStr);

        //create object inspectors from the type info for each column
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>();
        ObjectInspector oi;
        for (int c = 0; c < columnNames.size(); c++) {
            oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(c));
            columnOIs.add(oi);
        }

        objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
    }

    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    /**
     * row oi 为一个strcut 类型
     *
     * @param o
     * @param oi
     * @return
     * @throws SerDeException
     */
    public Writable serialize(Object o, ObjectInspector oi) throws SerDeException {
        return new Text("");
    }

    public SerDeStats getSerDeStats() {
        return null;
    }

    //数据反序列化
    public Object deserialize(Writable writable) throws SerDeException {
        //split writable object to row;
        if (writable == null) {
            return null;
        }

        Text text = (Text) writable;
        String[] fields = text.toString().split(FIELD_SEP);

        LOGGER.info("fields:{}", Arrays.asList(fields));
        if (fields.length != columnNames.size()) {
            LOGGER.info("fields num is not equal to columns num, fields:{}, col:{}", fields.length, columnNames.size());
            return null;
        }

        //构造对象
        ArrayList<Object> row = new ArrayList<Object>();
        TypeInfo typeInfo;
        Object fieldValue = null;
        for (int i = 0; i < columnNames.size(); i++) {
            typeInfo = columnTypes.get(i);
            if (typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
                PrimitiveTypeInfo p_typeInfo = (PrimitiveTypeInfo) typeInfo;
                switch (p_typeInfo.getPrimitiveCategory()) {
                    case STRING:
                        fieldValue = StringUtils.defaultString(fields[i], "");
                        break;
                    case DOUBLE:
                        fieldValue = Double.parseDouble(fields[i]);
                        break;
                    case INT:
                        fieldValue = Integer.parseInt(fields[i]);
                        break;
                }
            }

            LOGGER.info("field value:{}", fieldValue);
            row.add(fieldValue);
        }

        return row;
    }

    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }
}