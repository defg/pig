package org.apache.pig.piggybank.storage.arc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class PigArcInputFormat extends FileInputFormat<Text, PigArcRecord> {

    public PigArcInputFormat() {
    }

    public RecordReader<Text, PigArcRecord> createRecordReader(InputSplit split, TaskAttemptContext taskContext)
            throws IOException {

        Configuration config = taskContext.getConfiguration();
        return new PigArcRecordReader(config, (FileSplit)split);
    }
}
