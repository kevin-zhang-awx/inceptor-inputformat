package io.transwarp.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.datanucleus.store.types.backed.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * JSONSerde is based on https://github.com/cloudera/cdh-twitter-example
 * Created by zxh on 2017/5/24.
 */
public class JSONSerde extends AbstractSerDe {
    private static final Logger LOGGER = LoggerFactory.getLogger(JSONSerde.class);
    private StructTypeInfo rowTypeInfo;
    private ObjectInspector rowOI;
    private List<String> colNames;
    private List<TypeInfo> colTypeinfo;
    private List<Object> row = new ArrayList<Object>();


    public void initialize(Configuration configuration, Properties properties) throws SerDeException {
        String columnsStr = properties.getProperty(Constants.LIST_COLUMNS);
        colNames = Arrays.asList(columnsStr.split(","));

        String columnTypeStr = properties.getProperty(Constants.LIST_COLUMN_TYPES);
        colTypeinfo = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeStr);


        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>();
        ObjectInspector oi;
        for (int c = 0; c < colNames.size(); c++) {
            oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(colTypeinfo.get(c));
            columnOIs.add(oi);
        }

        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypeinfo);
        rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(colNames, columnOIs);
    }

    /**
     * 将json字符串反序列化为JSON对象，
     *
     * @param json
     * @return
     * @throws SerDeException
     */
    public Object deserialize(Writable json) throws SerDeException {
        Map<?, ?> root = null;
        row.clear();

        try {
            ObjectMapper mapper = new ObjectMapper();
            root = mapper.readValue(json.toString(), Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Object value = null;
        for (String fieldName : rowTypeInfo.getAllStructFieldNames()) {
            try {
                TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo(fieldName);
                value = parseField(root.get(fieldName), fieldTypeInfo);
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
                value = null;
            }

            row.add(value);
        }
        return row;
    }

    /**
     * 将JSON对象o，序列化为JSON字符串
     *
     * @param o
     * @param oi
     * @return
     * @throws SerDeException
     */
    public Writable serialize(Object o, ObjectInspector oi) throws SerDeException {
        //JSON对象本身就是Struct
        Object deparsedObject = deparseStruct(o, (StructObjectInspector) oi, true);
        ObjectMapper mapper = new ObjectMapper();
        Text text = null;
        try {
            text = new Text(mapper.writeValueAsString(deparsedObject));
        } catch (JsonProcessingException e) {
            LOGGER.error("Serialize JSON error, {}", e.getMessage());
        }
        return text;
    }


    /**
     * 解析JSON对象
     *
     * @param field
     * @param fieldTypeInfo
     * @return
     */
    public Object parseField(Object field, TypeInfo fieldTypeInfo) {
        switch (fieldTypeInfo.getCategory()) {
            case PRIMITIVE:
                if (field instanceof String) {
                    field = field.toString().replaceAll("\n", "\\\\n");
                }
                return field;
            case LIST:
                return parseList(field, (ListTypeInfo) fieldTypeInfo);
            case MAP:
                return parseMap(field, (MapTypeInfo) fieldTypeInfo);
            case STRUCT:
                return parseStruct(field, (StructTypeInfo) fieldTypeInfo);
            default:
                return null;
        }
    }

    public Object parseList(Object field, ListTypeInfo typeInfo) {
        ArrayList<Object> list = (ArrayList<Object>) field;
        TypeInfo elementType = typeInfo.getListElementTypeInfo();

        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                list.set(i, parseField(list.get(i), elementType));
            }
        }
        return list.toArray();
    }


    public Object parseMap(Object field, MapTypeInfo typeInfo) {
        Map<Object, Object> map = (Map<Object, Object>) field;
        TypeInfo valueTypeInfo = typeInfo.getMapValueTypeInfo();

        if (map != null) {
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                map.put(entry.getKey(), parseField(entry.getValue(), valueTypeInfo));
            }
        }

        return map;
    }

    public Object parseStruct(Object field, StructTypeInfo typeInfo) {
        Map<Object, Object> map = (Map<Object, Object>) field;
        ArrayList<TypeInfo> structTypes = typeInfo.getAllStructFieldTypeInfos();
        ArrayList<String> structNames = typeInfo.getAllStructFieldNames();

        List<Object> structRow = new ArrayList<Object>(structNames.size());

        if (map != null) {
            for (int i = 0; i < structNames.size(); i++) {
                structRow.add(parseField(map.get(structNames.get(i)), structTypes.get(i)));
            }
        }
        return structRow;
    }

    /**
     * 解析JSON对象
     *
     * @param object
     * @param oi
     * @return
     */
    public Object deparseJsonObject(Object object, ObjectInspector oi) {
        ObjectInspector.Category category = oi.getCategory();
        switch (category) {
            case PRIMITIVE:
                PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) oi;
                return primitiveObjectInspector.getPrimitiveJavaObject(object);
            case LIST:
                return deparseList(object, (ListObjectInspector) oi);
            case MAP:
                return deparseMap(object, (MapObjectInspector) oi);
            case STRUCT:
                return deparseStruct(object, (StructObjectInspector) oi, false);
            default:
                return null;
        }
    }


    public Object deparseList(Object object, ListObjectInspector oi) {
        List<Object> result = new ArrayList<Object>();
        List<Object> list = (List<Object>) oi.getList(object);
        ObjectInspector elementOI = oi.getListElementObjectInspector();
        for (Object o : list) {
            result.add(deparseJsonObject(o, elementOI));
        }
        return result;
    }

    public Object deparseMap(Object object, MapObjectInspector oi) {
        Map<String, Object> result = new HashMap<String, Object>();
        Map<String, ?> map = (Map<String, ?>) oi.getMap(object);
        ObjectInspector valueOI = oi.getMapValueObjectInspector();

        for (Map.Entry<String, ?> entry : map.entrySet()) {
            result.put(entry.getKey(), deparseJsonObject(entry.getValue(), valueOI));
        }
        return result;
    }

    public Object deparseStruct(Object object, StructObjectInspector oi, boolean isRow) {
        Map<Object, Object> structMap = new HashMap<Object, Object>();
        List<? extends StructField> structFields = oi.getAllStructFieldRefs();

        for (int i = 0; i < structFields.size(); i++) {
            StructField field = structFields.get(i);
            String fieldName = isRow ? colNames.get(i) : field.getFieldName();
            ObjectInspector fieldOI = field.getFieldObjectInspector();
            Object fieldObject = oi.getStructFieldData(object, field);
            structMap.put(fieldName, deparseJsonObject(fieldObject, fieldOI));
        }
        return structMap;
    }


    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }

    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }


    public SerDeStats getSerDeStats() {
        return null;
    }

}
