package org.apache.pig.piggybank.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.storage.arc.PigArcInputFormat;
import org.apache.pig.piggybank.storage.arc.PigArcRecord;
import org.apache.pig.piggybank.storage.arc.PigArcRecordReader;

import java.io.IOException;
import java.util.ArrayList;

public class ArcFileLoader extends LoadFunc {
    private static final Log log = LogFactory.getLog( ArcFileLoader.class );
    protected PigArcRecordReader _recordReader = null;
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    ResourceFieldSchema[] fields;
    Text key;
    PigArcRecord value;
    //String strSchema = "{}"
    
    /**
     * Constructs a Pig loader for the Common Crawl ARC file format.
     * See http://archive.org/web/researcher/ArcFileFormat.php for an ARC file format
     * specification and http://commoncrawl.org/data/accessing-the-data/ to understand
     * the Common Crawl data itself.
     */
    public ArcFileLoader() {
    }

    @Override
    public InputFormat<Text, PigArcRecord> getInputFormat() throws IOException {
        return new PigArcInputFormat();
    }

    @Override
    public Tuple getNext() throws IOException {
        log.warn("getNext()");
        Tuple t = tupleFactory.newTuple(4);
        try {
            boolean success = _recordReader.nextKeyValue();

            // True if we read the next record
            if(success) {
                key = _recordReader.getCurrentKey();
                value = _recordReader.getCurrentValue();

                log.warn("Url: " + key.toString());
                log.warn("Content: " + value.getContent());

                t = (Tuple) _recordReader.getCurrentValue();
                
                log.warn("content: " + t.toDelimitedString(", "));

                // Start by just returning the url
                t.set(1, key.toString());
                t.set(2, value.getIpAddress());
                t.set(3, value.getContent());
                t.set(4, value.getContentType());
                
            }
            else {
                log.warn("No success on nextKeyValue()");
            }

            return t;
        }
        catch (Exception ie) {
            throw new IOException(ie);
        }
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this._recordReader = (PigArcRecordReader) reader;
        log.info("Preparing to read with " + _recordReader);

        if(_recordReader == null)
            throw new IOException("Invalid Record Reader");

        // UDFContext udfc = UDFContext.getUDFContext();
        // Configuration c = udfc.getJobConf();
        // Properties p = udfc.getUDFProperties(this.getClass(), new String[]{_udfContextSignature});
        // ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
        // fields = schema.getFields();
    }

    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        PigArcInputFormat.setInputPaths(job, location);
    }
}