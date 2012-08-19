package org.apache.pig.piggybank.storage;

import org.apache.commons.logging.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.*;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldResponse;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.commoncrawl.hadoop.mapred.ArcRecord;
//import org.commoncrawl.hadoop.mapred.ArcRecordReader;
//import org.commoncrawl.hadoop.mapred.ArcInputFormat;
import org.apache.nutch.tools.arc.ArcInputFormat;
import org.apache.nutch.tools.arc.ArcRecordReader;
import org.apache.hadoop.mapred.FileInputFormat;
//import org.apache.hadoop.mapred.InputFormat;

import java.io.*;
import java.util.*;

public class ArcFileLoader extends LoadFunc {
    private static final Log log = LogFactory.getLog( ArcFileLoader.class );
    protected ArcRecordReader _recordReader = null;
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    ResourceFieldSchema[] fields;
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
    public Tuple getNext() throws IOException {
        Tuple t = tupleFactory.newTuple(1);
        try {
            Text key = null;
            ArcRecord value = null;
            boolean sucess = _recordReader.next(key, value);
            
            // True if we read the next record
            if(sucess) {
                log.info("Url: " + key.toString());
                log.info("Content: " + value.getContent());
            
                // Start by just returning the url
                t.set(1, key.toString());
            }
            
            return t;
        }
        catch (Exception ie) {
            throw new IOException(ie);
        }
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        this._recordReader = (ArcRecordReader) reader;
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
        ArcInputFormat.setInputPaths(job, location);
    }
}