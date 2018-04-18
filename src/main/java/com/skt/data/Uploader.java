package com.skt.data;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class Uploader {
    static private Logger log = Logger.getLogger(Uploader.class);

    public static void main(String[] args) throws IOException {
        Options options = new Options();
        options.addOption(Option.builder("inputPath")
                .required()
                .hasArg()
                .desc("Local input path (e.g. /local/path/to/input )")
                .type(String.class)
                .build()
        );
        options.addOption(Option.builder("outputPath")
                .required()
                .hasArg()
                .desc("Output path on HDFS (e.g. hdfs://namenode:8020/path/to/output")
                .type(String.class)
                .build()
        );
        options.addOption(Option.builder("tempFilePrefix")
                .optionalArg(true)
                .hasArg()
                .desc("Prefix of temp files to which small files are appended")
                .type(String.class)
                .build()
        );
        options.addOption(Option.builder("sizeLimit")
                .optionalArg(true)
                .hasArg()
                .desc("Files larger than this limit are directly copied to outputPath")
                .type(Integer.class)
                .build()
        );
        options.addOption(Option.builder("batchSizeLimit")
                .optionalArg(true)
                .hasArg()
                .desc("Limit of each batch")
                .type(Long.class)
                .build()
        );


        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Parsing failed. Reason: ", e);
            System.exit(1);
        }

        String inputPathString = cmd.getOptionValue("inputPath");
        String outputPathString = cmd.getOptionValue("outputPath");
        String tempFilePrefix = cmd.getOptionValue("tempFilePrefix", "seq-");
        int sizeLimit = Integer.parseInt(cmd.getOptionValue("sizeLimit", String.valueOf(Integer.MAX_VALUE)));
        long batchSizeLimit = Long.parseLong(cmd.getOptionValue("batchSizeLimit", String.valueOf(2*1024*1024*1024L)));

        log.info("inputPath : " + inputPathString);
        log.info("outputPath : " + outputPathString);
        log.info("tempFilePrefix : " + tempFilePrefix);
        log.info("sizeLimit : " + sizeLimit);
        log.info("batchSizeLimit : " + batchSizeLimit);

        // sanity check
        if (!outputPathString.startsWith("hdfs://")) {
            log.error("outputPath should start with hdfs:// but " + outputPathString);
            System.exit(1);
        }

        if (sizeLimit > batchSizeLimit) {
            log.error("sizeLimit cannot be larger than batchSizeLimit");
            System.exit(1);
        }

        long startTimeInMillis = System.currentTimeMillis();
        try {
            run(inputPathString, outputPathString, tempFilePrefix, sizeLimit, batchSizeLimit);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        long diff = System.currentTimeMillis() - startTimeInMillis;
        log.info("Elapsed time : " + diff);
    }

    private static void run(String inputPathString,
                            String outputPathString,
                            String tempFilePrefix,
                            int sizeLimit,
                            long batchSizeLimit) throws IOException{

        Configuration hadoopConf = new Configuration();
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(URI.create(outputPathString), hadoopConf);

        // Reusable buffers
        Text key = new Text();
        BytesWritable value = new BytesWritable();

        KeyValueBatchWriter writer = new KeyValueBatchWriter(
                outputPathString, tempFilePrefix, batchSizeLimit, hadoopConf);

        java.nio.file.Path inputPath = java.nio.file.Paths.get(inputPathString);
        java.nio.file.Files.walk(inputPath)
                .filter(java.nio.file.Files::isRegularFile)
                .filter(x->x.toFile().length() < sizeLimit)
                .forEach(path -> {
                    java.io.File file = path.toFile();
                    java.io.FileInputStream inputStream = null;
                    int fileSize = (int) file.length();
                    log.info("appending " + path + " (size : " + fileSize + ")");

                    key.set(inputPath.relativize(path).toString());
                    value.setSize(fileSize);
                    byte[] bytes = value.getBytes();

                    try {
                        inputStream = new FileInputStream(file);
                        inputStream.read(bytes, 0, fileSize);

                        writer.append(key, value);
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    } finally {
                        try {
                            if (inputStream != null)
                                inputStream.close();
                        } catch (IOException ioe2) {
                            ioe2.printStackTrace();
                        }
                    }
                });
        writer.close();

        System.out.println(
                "Files larger than " + sizeLimit +
                        " are directly uploaded on HDFS without aggregation");

        java.nio.file.Files.walk(inputPath)
                .filter(java.nio.file.Files::isRegularFile)
                .filter(x->x.toFile().length() >= sizeLimit)
                .forEach(path -> {
                    java.nio.file.Path relPath = inputPath.relativize(path);

                    org.apache.hadoop.fs.Path srcPath =
                            new org.apache.hadoop.fs.Path(path.toString());

                    org.apache.hadoop.fs.Path unqualifiedDestPath =
                            new org.apache.hadoop.fs.Path(outputPathString, relPath.toString());
                    org.apache.hadoop.fs.Path destPath =
                            fs.makeQualified(unqualifiedDestPath);

                    System.out.println(srcPath + " -> " + destPath);

                    try {
                        fs.copyFromLocalFile(srcPath, destPath);
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                });

    }
}
