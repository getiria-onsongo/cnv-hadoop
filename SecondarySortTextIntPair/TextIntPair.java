// cc TextIntPair A Writable implementation that stores a pair of Text objects

import java.io.*;
import java.lang.*;
import org.apache.hadoop.io.*;

public class TextIntPair implements WritableComparable<TextIntPair> {

  private Text first;
  private IntWritable second;
  
  public TextIntPair() {
        set(new Text(), new IntWritable());
    }
  
  public TextIntPair(Text first, IntWritable second) {
    set(first, second);
  }
  
  public void set(Text first, IntWritable second) {
    this.first = first;
    this.second = second;
  }
  
  public Text getFirst() {
    return first;
  }

  public IntWritable getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }
  
  @Override
  public int hashCode() {
    return first.hashCode() * 163 + second.hashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof TextIntPair) {
      TextIntPair tp = (TextIntPair) o;
      return first.equals(tp.first) && second.equals(tp.second);
    }
    return false;
  }

  @Override
  public String toString() {
    return first + "\t" + second;
  }
  
  @Override
  public int compareTo(TextIntPair tp) {
    int cmp = first.compareTo(tp.first);
    if (cmp != 0) {
      return cmp;
    }
    return second.compareTo(tp.second);
  }
}
// ^^ TextIntPair
