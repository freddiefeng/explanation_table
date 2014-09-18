package etbutil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.Iterator;

@Description(name = "powerset")
public class GenericUDTFPowerSet extends GenericUDTF {
//    private static Log LOG = LogFactory.getLog(GenericUDTFPowerSet.class.getName());

    int numFields = 0;
    private transient Object[] inputOIs;
    String[] pattern;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        inputOIs = args;
        numFields = args.length;

        ArrayList<String> fieldNames = new ArrayList<String>(numFields);
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(numFields);
        for (int i = 0; i < numFields; i++) {
            fieldNames.add("a" + i);
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        pattern = new String[numFields];

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    private ArrayList<String[]> generateAncestors(String[] source, int idx) {
        if (idx == source.length) {
            return new ArrayList<String[]>();
        } else {
            ArrayList<String[]> buffer = generateAncestors(source, idx + 1);

            if (buffer.isEmpty()) {
                buffer.add(source);
            }

            if (null == source[idx]) {
                return buffer;
            } else {
                ArrayList<String[]> ancestorBuffer = new ArrayList<String[]>();

                for (Iterator<String[]> iter = buffer.iterator(); iter.hasNext(); ) {
                    String[] item = iter.next();
                    String[] parent = new String[item.length];
                    System.arraycopy(item, 0, parent, 0, item.length);
                    parent[idx] = null;
                    ancestorBuffer.add(parent);
                }

                buffer.addAll(ancestorBuffer);
                return buffer;
            }
        }
    }

    @Override
    public void process(Object[] objects) throws HiveException {
//        LOG.debug("================================process");
        assert(objects.length == numFields);
        for (int i = 0; i < numFields; i++) {
            pattern[i] = ((StringObjectInspector) inputOIs[i]).getPrimitiveJavaObject(objects[i]);
        }

        ArrayList<String[]> allAncestors = generateAncestors(pattern, 0);
        for (Iterator<String[]> iter = allAncestors.iterator(); iter.hasNext(); ) {
            forward(iter.next());
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
