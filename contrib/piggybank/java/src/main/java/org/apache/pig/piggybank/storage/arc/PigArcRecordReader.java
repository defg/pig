/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.piggybank.storage.arc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;

// - modified by Common Crawl -
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * <p>The <code>ArcRecordReader</code> class provides a record reader which
 * reads records from arc files.</p>
 *
 * <p>Arc files are essentially tars of gzips.  Each record in an arc file is
 * a compressed gzip.  Multiple records are concatenated together to form a
 * complete arc.  For more information on the arc file format see
 * {@link ://www.archive.org/web/researcher/ArcFileFormat.php } .</p>
 *
 * <p>Arc files are used by the internet archive and grub projects.</p>
 *
 * see {@link ://www.archive.org/ }
 * see {@link ://www.grub.org/ }
 */
public class PigArcRecordReader extends RecordReader<Text, PigArcRecord> {

    private static final Logger LOG = Logger.getLogger(PigArcRecordReader.class);

    private NutchArcRecordReader _impl;

    private Text key = null;
    private PigArcRecord value = null;


    /**
     /**
     * {@inheritDoc}
     */
    public PigArcRecordReader(Configuration conf, FileSplit split)
            throws IOException {
        this._impl = new NutchArcRecordReader(conf, split);
        LOG.setLevel(Level.DEBUG);
    }

    /**
     * {@inheritDoc}
     */
    public void close()
            throws IOException {
        this._impl.close();
    }

    /**
     * Creates a new instance of the <code>Text</code> object for the key.
     */
    public Text getCurrentKey() {
        return new Text();
    }

    /**
     * Creates a new instance of the <code>BytesWritable</code> object for the key
     */
    public PigArcRecord getCurrentValue() {
        return new PigArcRecord();
    }

    /**
     * Returns the current position in the file.
     *
     * @return The long of the current position in the file.
     */
    public long getPos()
            throws IOException {
        return this._impl.getPos();
    }

    /**
     * Returns the percentage of progress in processing the file.  This will be
     * represented as a float from 0 to 1 with 1 being 100% completed.
     *
     * @return The percentage of progress as a float from 0 to 1.
     */
    public float getProgress()
            throws IOException {
        return this._impl.getProgress();
    }

    private final int _maxRecursion = 100;
    private       int _recursion    = 0;

    private boolean _callNext(Text key, PigArcRecord value)
            throws IOException {
        boolean rv;
        this._recursion++;
        rv = this.nextKeyValue();
        this._recursion--;
        return rv;
    }

    public boolean nextKeyValue()
            throws IOException {

        BytesWritable bytes = new BytesWritable();

        boolean rv;

        // get the next record from the underlying Nutch implementation
        rv = this._impl.nextKeyValue();
        LOG.warn("impl.nextKeyValue: " + Boolean.toString(rv));
        key = this._impl.getCurrentKey();
        LOG.warn("Key: " + key.toString());
        bytes = this._impl.getCurrentValue();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Entering RecordReader.next() - recursion = " + this._recursion);
            LOG.debug("- ARC Record Header (Nutch): [" + key.toString() + "]");
            LOG.debug("- ARC Record Content Size (Nutch):  " + String.format("%,12d", bytes.getLength()));
            LOG.debug("- Free / Curr JVM / Max JVM Memory: " + String.format("%,12d", Runtime.getRuntime().freeMemory()  / 1024 / 1024) + "MB "
                    + String.format("%,12d", Runtime.getRuntime().totalMemory() / 1024 / 1024) + "MB "
                    + String.format("%,12d", Runtime.getRuntime().maxMemory()   / 1024 / 1024) + "MB");
        }

        // if 'false' is returned, EOF has been reached
        if (rv == false) {
            if (LOG.isDebugEnabled())
                LOG.debug("Nutch ARC reader returned FALSE at " + this.getPos());
            return false;
        }

        // if we've found too many invalid records in a row, bail ...
        if (this._recursion > this._maxRecursion) {
            LOG.error("Found too many ("+this._maxRecursion+") invalid records in a row.  Aborting ...");
            return false;
        }

        // get the ARC record header returned from Nutch
        String arcRecordHeader = key.toString();

        // perform a basic sanity check on the record header
        if (arcRecordHeader.length() < 12) {
            LOG.error("Record at offset " + this.getPos() + " does not have appropriate ARC header: [" + arcRecordHeader + "]");
            return this._callNext(key, value);
        }

        // skip the ARC file header record
        if (arcRecordHeader.startsWith("filedesc://")) {
            LOG.info("File header detected: skipping record at " + this.getPos() + " [ " + arcRecordHeader + "]");
            return this._callNext(key, value);
        }

        try {

            // split ARC metadata into fields
            value.setArcRecordHeader(arcRecordHeader);

            if (LOG.isDebugEnabled())
                LOG.debug("- ARC Record Size (ARC Header):     " + String.format("%,12d", value.getContentLength()));

            // set the key to the URL
            key.set(value.getURL());

            // see if we need to parse HTTP headers
            if (arcRecordHeader.startsWith("http://")) {
                value.setParseHttpMessage(true);
            }

            // set the content, and parse the headers (if necessary)
            value.setContent(bytes);
        }
        catch (IllegalArgumentException ex) {
            LOG.error("Unable to process record at offset " + this.getPos() + ": ", ex);
            return this._callNext(key, value);
        }
        catch (ParseException ex) {
            LOG.error("Unable to process record at offset " + this.getPos() + ": ", ex);
            return this._callNext(key, value);
        }

        return true;
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
        // Nothing to do
    }

}
