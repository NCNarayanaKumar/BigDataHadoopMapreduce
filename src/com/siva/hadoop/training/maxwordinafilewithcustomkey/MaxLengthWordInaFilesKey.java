package com.siva.hadoop.training.maxwordinafilewithcustomkey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MaxLengthWordInaFilesKey implements WritableComparable<MaxLengthWordInaFilesKey> {

	private String fileName;
	private String word;
	private long length;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fileName == null) ? 0 : fileName.hashCode());
		result = prime * result + (int) (length ^ (length >>> 32));
		result = prime * result + ((word == null) ? 0 : word.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MaxLengthWordInaFilesKey other = (MaxLengthWordInaFilesKey) obj;
		if (fileName == null) {
			if (other.fileName != null)
				return false;
		} else if (!fileName.equals(other.fileName))
			return false;
		if (length != other.length)
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	@Override
	public String toString() {
		return "[fileName=" + fileName + ", word=" + word + ", length=" + length + "]";
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		fileName = in.readUTF();
		word = in.readUTF();
		length = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(fileName);
		out.writeUTF(word);
		out.writeLong(length);
	}

	@Override
	public int compareTo(MaxLengthWordInaFilesKey key) {
		int diff = fileName.compareTo(key.fileName);
		if (diff == 0) {
			diff = word.compareTo(key.word);
			if (diff == 0) {
				long d = length - key.length;
				if (d > 0)
					return 1;
				else if (d < 0)
					return -1;
				else
					return 0;
			}
		}
		return diff;
	}

}
