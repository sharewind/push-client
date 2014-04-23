package com.sohu.push;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Test {

	public static void main(String[] args) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(4);
		byteBuffer.asIntBuffer().put(199);
		byte[] bytes = byteBuffer.array();
		System.out.println(Arrays.toString(bytes));

		ByteBuffer byteBuffer2 = ByteBuffer.wrap(bytes);
		int result = byteBuffer2.asIntBuffer().get();
		System.out.println(result);
	}

}
