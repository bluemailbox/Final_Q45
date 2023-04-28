package textdb.util;

import java.math.BigDecimal;

/**
 * Contains static methods for converting memory types to byte arrays and back. 
 */
public class Convert
{
	/*
	Methods to convert to byte arrays
	*/
	public static final byte[] toByte(short sh)						// Short
	{
		byte ba[] = new byte[2];
		for (byte b = 0; b <= 1; b++)
			ba[b] = (byte)(sh >>> (1 - b) * 8);
		return ba;
	}

	public static final byte[] toByte(int i) 						// Integer
	{
		byte ba[] = new byte[4];
		for (byte b = 0; b <= 3; b++)
			ba[b] = (byte)(i >>> (3 - b) * 8);
		return ba;
	}

	public static final byte[] toByte(String s)						// String
	{
			return s.getBytes();
	}


	/*
	Methods to convert from byte arrays
	*/

	public static final int toInt(byte ba[])						// Integer
	{
		int i = 0;

		for (byte b = 0; b <= 3; b++)
		{
			int j;
			if (ba[b] < 0) {
				ba[b] = (byte)(ba[b] & 0x7f);
				j = ba[b];
				j |= 0x80;
			}
			else {
				j = ba[b];
			}
			i |= j;
			if (b < 3)
				i <<= 8;
		}
		return i;
	}

	public static final short toShort(byte ba[]) 					// Short
	{
		short word0 = 0;

		for (byte b = 0; b <= 1; b++) {
			short word1;
			if (ba[b] < 0) {
				ba[b] = (byte)(ba[b] & 0x7f);
				word1 = ba[b];
				word1 |= 0x80;
			}
			else {
				word1 = ba[b];
			}
			word0 |= word1;
			if(b < 1)
				word0 <<= 8;
		}
		return word0;
	}

	public static final String toString(byte ba[], int len) 		// String
	{
		return new String(ba, 0, len);
	}

	public static final BigDecimal toBigDecimal(byte ba[], int len) 		// String
	{	String st = new String(ba, 0, len);
		return new BigDecimal(st);
	}
}
