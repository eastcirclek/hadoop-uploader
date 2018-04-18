# hadoop-uploader

## Introduction
**hadoop-uploader** is a file uploader specialized for uploading many small files to Hadoop. 
We use **hadoop-uploader** to copy small files (20MB on average) stored on an external hard drive to HDFS. 
While ```hdfs dfs -put``` takes around 3 days to upload 10TB, **hadoop-uploader**  takes less than 12 hours. 

Uploading many small files onto HDFS using ```hdfs dfs -put``` could suffer from very low network and disk bandwidth.
The HDFS protocol tells a client to contact with a namenode every time it initiates a procedure for copying a file, thus preventing the client from spending time purely on uploading file contents.
Launching multiple HDFS clients could partly alleviate such a problem but it can introduce random access which can cause severe performance problems on hard disks.

**hadoop-uploader** is designed to benefit from sequential read speed of the source file system and benefit from computing power on the target Hadoop cluster.

## How it works

**hadoop-uploader** consists of two phases, **upload** and **extract**. Upload tasks are executed on the client side while extract tasks are executed on the Hadoop cluster.
The execution of the two phases can be overlapped so the entire uploading job can be done around the time when the upload phase is finished. 

A upload phase partitions a set of files into smaller sets, and each set is uploaded to HDFS by a upload task.
A upload task creates a Hadoop **SequenceFile** on the target HDFS and appends the files to the SequenceFile by using the file name as key and the content as value.
Upload tasks are executed sequentially so that only a single thread reads from the source file system to enjoy sequential read speed of a disk.
This upload phase is designed to avoid expensive communication costs by creating a small number of SequenceFile on HDFS.
For instance, when given 10,000 files, a upload task creates only a single SequenceFile on HDFS instead of creating 10,000 separate files. 
As we put the file content into **BytesWritable**, a upload task copies the files that are larger than 2GB directly to HDFS.

Whenever a upload task is done, a extract task needs to be executed on the Hadoop cluster to extract files stored on SequenceFile.
The extract step is a simple Spark application which takes a SequenceFile as input and iterates through key-value pairs to create individual files on the HDFS.
Considering the recommendation of the block size is 128MB or 256MB, an intermediate file of few hundreds of GBs can be processed by hundreds of Spark tasks over the cluster.
In this way, **hadoop-uploader** tries to eliminate the overhead of this additional phase by using computing power of the target Hadoop cluster. 