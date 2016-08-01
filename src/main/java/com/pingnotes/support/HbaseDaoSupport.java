package com.pingnotes.support;

import com.alibaba.fastjson.JSON;
import com.pingnotes.annotation.Column;
import com.pingnotes.annotation.JsonField;
import com.pingnotes.annotation.MapField;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.IncompleteAnnotationException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;


/**
 * Created by shaobo on 7/29/16.
 */
public class HbaseDaoSupport<T extends HbaseBaseDo> {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseDaoSupport.class);
    private static Configuration conf = HbaseConfig.getConfig();

    private String tableName;
    private Class<?> clz;

    public HbaseDaoSupport(String tableName, Class<?> clz) {
        this.tableName = tableName;
        this.clz = clz;
    }

    protected void insert(T obj) {
        Connection connection = null;
        Table table = null;
        if (!obj.hasRowKey()) {
            return;
        }
        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(tableName));
            table.put(doPut(obj));
        } catch (IOException e) {
            LOG.error("connection error when insert", e);
        } catch (IllegalAccessException e) {
            LOG.error("put field error when insert", e);
        } finally {
            closeAll(table, connection);
        }
    }

    protected void delete(String rowkey) {
        if (rowkey == null || rowkey.isEmpty()) {
            return;
        }
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(rowkey.getBytes());
            table.delete(del);
        } catch (IOException e) {
            LOG.error("connection error when delete", e);
        } finally {
            closeAll(table, connection);
        }
    }

    protected void update(T obj) {
        if (!obj.hasRowKey()) {
            return;
        }

        Connection connection = null;
        Table table = null;
        long now = System.currentTimeMillis();
        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(obj.rowKeyBytes(), now);
            Put put = new Put(obj.rowKeyBytes(), now + 1);
            for (Field field : obj.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                if (field.isAnnotationPresent(JsonField.class)) {
                    jsonBytesPut(put, obj, field);
                } else if (field.isAnnotationPresent(MapField.class)) {
                    del.addFamily(field.getAnnotation(Column.class).family().getBytes());
                    mapPut(put, obj, field);
                } else {
                    pojoPut(put, obj, field);
                }
                //// TODO: 7/30/16 POJO field
            }
            //HBASE-8626
            RowMutations rowMutations = new RowMutations(obj.rowKeyBytes());
            rowMutations.add(del);
            rowMutations.add(put);
            table.mutateRow(rowMutations);
        } catch (IllegalAccessException e) {
            LOG.error("put error when update", e);
        } catch (IOException e) {
            LOG.error("connection error when update", e);
        } finally {
            closeAll(table, connection);
        }
    }

    protected T query(String rowkey) {
        if (rowkey == null || rowkey.isEmpty()) {
            return null;
        }
        Get get = new Get(rowkey.getBytes());
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(tableName));
            Result result = table.get(get);
            return buildDo(result);
        } catch (IOException e) {
            LOG.error("connection error when query", e);
            return null;
        } finally {
            closeAll(table, connection);
        }
    }


    /**
     * @param obj, not null & has row key
     * @return
     * @throws IllegalAccessException
     */
    private Put doPut(T obj) throws IllegalAccessException {
        Put put = new Put(obj.rowKeyBytes());
        for (Field field : obj.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(JsonField.class)) {
                jsonBytesPut(put, obj, field);
            } else if (field.isAnnotationPresent(MapField.class)) {
                mapPut(put, obj, field);
            } else {
                pojoPut(put, obj, field);
            }
            //// TODO: 7/30/16 PojoField
        }
        return put;
    }

    /**
     * @param Obj,   not null
     * @param field, null
     * @throws IllegalAccessException
     */
    private void fieldNullCheck(T Obj, Field field) throws IllegalAccessException {
        if (field.get(Obj) == null) {
            throw new IllegalStateException("object field " + field.getName() + " not initialized");
        }
    }

    private void columnCheck(Field field) {
        if (!field.isAnnotationPresent(Column.class)) {
            throw new IncompleteAnnotationException(Column.class, "Column annotation required");
        }
        String family = field.getAnnotation(Column.class).family();
        String qualifier = field.getAnnotation(Column.class).qualifier();
        if (family.isEmpty() || qualifier.isEmpty()) {
            throw new IncompleteAnnotationException(Column.class, "Column annotation value is null");
        }
    }

    private Object typeCast(Class<?> clz, byte[] bytes) {
        if (clz.equals(String.class)) {
            return Bytes.toString(bytes);
        } else if (clz.equals(Integer.class)) {
            return Bytes.toInt(bytes);
        } else if (clz.equals(Long.class)) {
            return Bytes.toLong(bytes);
        } else if (clz.equals(Boolean.class)) {
            return Bytes.toBoolean(bytes);
        } else if (clz.equals(Double.class)) {
            return Bytes.toDouble(bytes);
        } else if (clz.equals(Byte.class)) {
            return bytes[0];
        } else {
            throw new IllegalArgumentException("class type " + clz.getName() + " not support");
        }
    }

    private byte[] typeCast(Object obj) {
        if (obj instanceof Integer) {
            return Bytes.toBytes((Integer) obj);
        } else if (obj instanceof String) {
            return Bytes.toBytes((String) obj);
        } else if (obj instanceof Long) {
            return Bytes.toBytes((Long) obj);
        } else if (obj instanceof Boolean) {
            return Bytes.toBytes((Boolean) obj);
        } else if (obj instanceof Float) {
            return Bytes.toBytes((Float) obj);
        } else if (obj instanceof Byte) {
            return Bytes.toBytes((Byte) obj);
        } else if (obj instanceof Character) {
            return Bytes.toBytes((Character) obj);
        } else if (obj instanceof Double) {
            return Bytes.toBytes((Double) obj);
        } else {
            throw new IllegalArgumentException("field type " + obj.getClass().getName() + " not support");
        }
    }

    /**
     * fill Put with the Field of (T) obj
     *
     * @param put   [in/out]
     * @param obj
     * @param field
     * @return
     * @throws IllegalAccessException
     */
    private Put pojoPut(Put put, final T obj, final Field field) throws IllegalAccessException {
        if (!field.isAnnotationPresent(Column.class)) {
            return put;
        }
        Column column = field.getAnnotation(Column.class);
        String family = column.family();
        String qualifier = column.qualifier();
        Object fieldObj = field.get(obj);
        if (family.isEmpty() || qualifier.isEmpty() || fieldObj == null) {
            return put;
        }
        if (fieldObj instanceof Integer) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Integer) fieldObj));
        } else if (fieldObj instanceof Boolean) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Boolean) fieldObj));
        } else if (fieldObj instanceof Long) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Long) fieldObj));
        } else if (fieldObj instanceof Float) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Float) fieldObj));
        } else if (fieldObj instanceof String) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((String) fieldObj));
        } else if (fieldObj instanceof Byte) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Byte) fieldObj));
        } else if (fieldObj instanceof Double) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Double) fieldObj));
        } else {
            throw new IllegalArgumentException("field type " + fieldObj.getClass().getName() + " not support");
        }
        return put;
    }

    /**
     * fill Put with the Field of (T) obj;
     *
     * @param put   [in/out]
     * @param obj
     * @param field
     * @return
     * @throws IllegalAccessException
     */
    private Put jsonBytesPut(Put put, final T obj, final Field field) throws IllegalAccessException {
        if (!field.isAnnotationPresent(Column.class)) {
            return put;
        }
        Column column = field.getAnnotation(Column.class);
        if (column.family().isEmpty() || column.qualifier().isEmpty()) {
            return put;
        }
        Object var = field.get(obj);
        if (var == null) {
            return put;
        }
        put.addColumn(column.family().getBytes(), column.qualifier().getBytes(), JSON.toJSONBytes(var));
        return put;
    }

    /**
     * fill Put with the Field of (T) obj
     *
     * @param put   [in/out]
     * @param obj
     * @param field
     * @return
     * @throws IllegalAccessException
     */
    private Put mapPut(Put put, final T obj, final Field field) throws IllegalAccessException {
        if (!field.isAnnotationPresent(Column.class)) {
            return put;
        }
        String family = field.getAnnotation(Column.class).family();
        if (family.isEmpty()) {
            return put;
        }

        Map<Object, Object> mp = (Map<Object, Object>) field.get(obj);
        if (mp == null || mp.isEmpty()) {
            return put;
        }
        for (Map.Entry<Object, Object> entry : mp.entrySet()) {
            put.addColumn(family.getBytes(), typeCast(entry.getKey()), typeCast(entry.getValue()));
        }
        return put;
    }

    private Put doUpdate(T obj) throws IllegalAccessException {
        Put put = new Put(obj.rowKeyBytes());

        for (Field field : obj.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(JsonField.class)) {
                jsonBytesPut(put, obj, field);
            } else {
                pojoPut(put, obj, field);
            }
            //// TODO: 7/30/16 POJO field
        }
        return put;
    }

    private void closeAll(Table table, Connection connection) {
        try {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            LOG.error("close error", e);
        }
    }

    /**
     * handle json field of (T) obj
     * @param stu    [in/out]
     * @param field, with annotation JsonField
     * @param result
     * @throws IllegalAccessException
     */
    private void parseJsonBytes(T stu, Field field, Result result) throws IllegalAccessException {
        if (!field.isAnnotationPresent(Column.class)) {
            return;
        }
        Column column = field.getAnnotation(Column.class);
        if (column.qualifier().isEmpty() || column.family().isEmpty()) {
            return;
        }
        byte[] val = result.getValue(column.family().getBytes(), column.qualifier().getBytes());
        if (val == null) {
            return;
        }
        Class eleClz = field.getAnnotation(JsonField.class).elementClass();
        field.set(stu, JSON.parseArray(new String(val), eleClz));
    }

    private void parseMap(T stu, Field field, Result result) throws IllegalAccessException {
        if (!field.isAnnotationPresent(Column.class)){
            return;
        }
        Column column = field.getAnnotation(Column.class);
        if (column.family().isEmpty()){
            return;
        }
        MapField mapFieldAnnotation = field.getAnnotation(MapField.class);
        Class<?> keyClz = mapFieldAnnotation.keyClass();
        Class<?> valClz = mapFieldAnnotation.valueClass();
        NavigableMap<byte[], byte[]> mp = result.getFamilyMap(column.family().getBytes());
        Map<Object, Object> mapField = new HashMap<Object, Object>();
        for (Map.Entry<byte[], byte[]> entry : mp.entrySet()) {
            mapField.put(typeCast(keyClz, entry.getKey()), typeCast(valClz, entry.getValue()));
        }
        field.set(stu, mapField);
    }

    private void fillField(T obj, final Field field, final Result result) throws IllegalAccessException {
        Column column = field.getAnnotation(Column.class);
        if (field.isAnnotationPresent(JsonField.class)) {
            parseJsonBytes(obj, field, result);
        } else if (field.isAnnotationPresent(MapField.class)) {
            parseMap(obj, field, result);
        } else {
            byte[] val = result.getValue(column.family().getBytes(), column.qualifier().getBytes());
            if (val == null) {
                return;
            }
            Class type = field.getType();
            if (type.equals(Integer.class)) {
                field.set(obj, Bytes.toInt(val));
            } else if (type.equals(String.class)) {
                field.set(obj, Bytes.toString(val));
            } else if (type.equals(Long.class)) {
                field.set(obj, Bytes.toLong(val));
            } else if (type.equals(Boolean.class)) {
                field.set(obj, Bytes.toBoolean(val));
            } else if (type.equals(Double.class)) {
                field.set(obj, Bytes.toDouble(val));
            } else if (type.equals(Float.class)) {
                field.set(obj, Bytes.toFloat(val));
            } else if (type.equals(Byte.class)) {
                field.set(obj, val[0]);
            } else {
                throw new IllegalArgumentException("field type " + type.getName() + " is not support");
            }

        }
    }

    private T buildDo(Result result) {
        T obj = null;
        if (result == null || result.isEmpty() || clz == null) {
            return obj;
        }
        try {
            obj = (T) clz.newInstance();
            for (Field field : obj.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                fillField(obj, field, result);
            }
            return obj;
        } catch (InstantiationException e) {
            LOG.error("new instance error when buildDo", e);
            return null;
        } catch (IllegalAccessException e) {
            LOG.error("some error", e);
            return null;
        }
    }

}
