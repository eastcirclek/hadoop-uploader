package com.skt.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;

public class KeyValueBatchWriter {
    private Logger log = Logger.getLogger(KeyValueBatchWriter.class);

    private int batchCounts = 0;
    private long currentBatchSize = 0L;

    private String outputPath;
    private String filePrefix;
    private long batchSizeLimit;
    private Configuration hadoopConf;

    private FileSystem fs;
    private SequenceFile.Writer writer = null;

    public KeyValueBatchWriter(String outputPath,
                               String filePrefix,
                               long batchSizeLimit,
                               Configuration hadoopConf) throws IOException {
        this.outputPath = outputPath;
        this.filePrefix = filePrefix;
        this.batchSizeLimit = batchSizeLimit;
        this.hadoopConf = hadoopConf;

        this.fs = FileSystem.get(URI.create(outputPath), hadoopConf);

        nextWriter();
    }

    public void nextWriter() throws IOException {
        if (writer != null) {
            writer.close();
        }

        Path filePath = new Path(this.outputPath, this.filePrefix+this.batchCounts);
        if (fs.exists(filePath)) {
            fs.delete(filePath, true);
        }

        log.info("nextWriter : " + filePath);

        this.writer = SequenceFile.createWriter(
                this.hadoopConf,
                SequenceFile.Writer.file(filePath),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(BytesWritable.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE)
        );

        this.batchCounts++;
        this.currentBatchSize = 0L;
    }

    public void append(Text key, BytesWritable value) throws IOException {
        if (this.currentBatchSize + value.getLength() > this.batchSizeLimit) {
            nextWriter();
        }

        if (this.writer != null) {
            this.writer.append(key, value);
            this.currentBatchSize += value.getLength();
        }
    }

    public void close() throws IOException {
        if (this.writer != null) {
            this.writer.close();
        }
    }
}
