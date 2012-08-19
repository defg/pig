package org.apache.pig.piggybank.storage.arc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.nutch.tools.arc.ArcInputFormat;
import org.apache.nutch.tools.arc.ArcRecordReader;
import org.commoncrawl.hadoop.mapred.ArcRecord;

import java.io.IOException;

public class PigArcInputFormat extends FileInputFormat<Text, ArcRecord> {

    public PigArcInputFormat() {
    }

    public ArcInputFormat getInputFormat() throws IOException {
        return new ArcInputFormat();
    }

    public RecordReader<Text, ArcRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        Configuration conf = context.getConfiguration();
        return new ArcRecordReader(conf, (FileSplit)split);
    }
}
