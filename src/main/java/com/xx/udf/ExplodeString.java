package com.xx.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description TODO
 * @Author hehuanwen
 * @Date 2021-05-26 16:01
 */
public class ExplodeString extends GenericUDTF {
    private ArrayList<String> arrayList = new ArrayList();

    //argOIs里面有输入参数的objectinspector,如果输入参数有两个就有两个inspector,不过这里只有一个参数
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<? extends StructField> allStructFieldRefs = argOIs.getAllStructFieldRefs();
        if (allStructFieldRefs.size() != 2) {
            throw new UDFArgumentException("需要两个参数");
        }
        ObjectInspector inputobjectinspector = allStructFieldRefs.get(0).getFieldObjectInspector();
        if (!((PrimitiveObjectInspector)inputobjectinspector).getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.STRING)) {
            throw new UDFArgumentException("第一个参数为字符串");
        }
        //设置返回字段的默认字段名和对应的objectinspector
        ArrayList<String> outputFieldName = new ArrayList(2);
        ArrayList<ObjectInspector> objectInspectors = new ArrayList(2);
        outputFieldName.add("first");
        outputFieldName.add("second");
        objectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        objectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(outputFieldName, objectInspectors);
    }

    @Override
    //args为一行的输入参数,第一个字段为args[0],第二个字段为args[1],一次类推
    public void process(Object[] args) throws HiveException {
        String fieldone = String.valueOf(args[0]);
        String fieldtwo = String.valueOf(args[1]);
        String[] split = fieldone.split(fieldtwo);
        for (String s : split) {
            arrayList.clear();
            String[] kvs = s.split(":");
            String kv = kvs[0];
            String kv1 = kvs[1];
            arrayList.add(kv);
            arrayList.add(kv1);
            forward(arrayList);
        }


    }

    @Override
    public void close() throws HiveException {

    }
}
