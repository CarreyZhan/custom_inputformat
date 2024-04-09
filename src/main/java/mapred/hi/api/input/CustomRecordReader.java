package mapred.hi.api.input;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

public class CustomRecordReader implements RecordReader<LongWritable, Text>{
    private static final Log LOG = LogFactory.getLog(CustomRecordReader.class);
    public static final String MAX_LINE_LENGTH = 
      "mapreduce.input.linerecordreader.line.maxlength";
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private long start;
    private long pos;
    private long end;
    private CustomReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private int maxLineLength;
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private byte[] recordDelimiterBytes;
      
    public CustomRecordReader(Configuration conf, InputSplit split) throws IOException {
        String delimiter = conf.get("inputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter)
          recordDelimiterBytes = delimiter.getBytes();
        assert recordDelimiterBytes != null : "should set inputformat.record.delimiter";
        this.recordDelimiterBytes = recordDelimiterBytes;
        LOG.info("using delimiter:" + recordDelimiterBytes);
        this.initialize(split, conf);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
          if (in != null) {
            in.close();
          }
        } finally {
          if (decompressor != null) {
            CodecPool.returnDecompressor(decompressor);
          }
        }
      }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text(new byte[DEFAULT_BUFFER_SIZE]);
    }

    @Override
    public float getProgress() {
        if (this.start == this.end) {
              return 0.0F;
            }
            return Math.min(1.0F, (float)(this.pos - this.start) / (float)(this.end - this.start));
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    public void initialize(InputSplit genericSplit, Configuration conf)
            throws IOException {

        FileSplit split = (FileSplit) genericSplit;
        Configuration job = conf;
        this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        
        CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
        boolean skipFirstLine = false;
        if (codec != null) {
          this.in = new CustomReader(codec.createInputStream(fileIn), job,recordDelimiterBytes);
          this.end = 9223372036854775807L;
        } else {
          if (this.start != 0L) {
            skipFirstLine = true;
            this.start -= 1L;
            fileIn.seek(this.start);
          }
          this.in = new CustomReader(fileIn, job,recordDelimiterBytes);
        }
        if (skipFirstLine) {
          this.start += this.in.readObject(new Text(new byte[DEFAULT_BUFFER_SIZE]), 0, (int)Math.min(2147483647L, this.end - this.start));
        }

        this.pos = this.start;
    }

    @Override
    public boolean next(LongWritable key, Text value) throws IOException {

        key.set(this.pos);
        int newSize = 0;
        while (this.pos < this.end) {
          newSize = this.in.readObject(value, this.maxLineLength, Math.max((int)Math.min(2147483647L, this.end - this.pos), this.maxLineLength));

          if (newSize == 0) {
            break;
          }
          this.pos += newSize;
          if (newSize < this.maxLineLength)
          {
            break;
          }

          LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - newSize));
        }

        if (newSize == 0) {
          key = null;
          value = null;
          return false;
        }
        return true;
    }

}
