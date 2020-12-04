package de.tuda.dmdb.storage.types.exercise;

import de.tuda.dmdb.storage.types.SQLIntegerBase;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;

/**
 * SQL integer value
 * 
 * @author cbinnig
 *
 */
public class SQLInteger extends SQLIntegerBase {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor with default value
	 */
	public SQLInteger() {
		super();
	}

	/**
	 * Constructor with value
	 * 
	 * @param value
	 *            Integer value
	 */
	public SQLInteger(int value) {
		super(value);
	}

	@Override
	public byte[] serialize() {
		// TODO: implement this method
		//Java uses signed bytes
		byte[] signedData = new byte[4];
		//right shift and save data as big endian array to serialize
		signedData[0] = (byte) (this.value >> 24);
		signedData[1] = (byte) (this.value >> 16);
		signedData[2] = (byte) (this.value >> 8);
		signedData[3] = (byte) (this.value);

		return signedData;
	}

	@Override
	public void deserialize(byte[] data) {
		// TODO: implement this method
		// this.value = ?;

		// signed bytes to unsigned bytes
		int[] unsignedData = new int[data.length];
		for (int i = 0; i < data.length; i++)
			unsignedData[i] = data[i] & 0xff;

		//read array as big endian, shift values to the left to deserialize and append them wit bitwise or
		this.value = unsignedData[3] | unsignedData[2] << 8 | unsignedData[1] << 16 | unsignedData[0] << 24;
	}

	@Override
	public SQLInteger clone() {
		return new SQLInteger(this.value);
	}
}
