import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable{
		IntWritable key = new IntWritable();
		IntWritable value = new IntWritable();
		
		public Pair(){}
		
		public Pair(IntWritable k, IntWritable v){
			this.key = k;
			this.value = v;
		}
		
		public void setKey(IntWritable k){
			this.key = k;
		}
		
		public void setValue(IntWritable v){
			this.value = v;
		}
		
		public IntWritable getKey(){
			return key;
		}
		
		public IntWritable getValue(){
			return value;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			key.readFields(arg0);
			value.readFields(arg0);
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			key.write(arg0);
			value.write(arg0);
		}

		@Override
		public int compareTo(Object o) {
			if(o instanceof Pair)
				return ((Pair)o).getKey().compareTo(key);
			else
				return -1;
		}		
	}