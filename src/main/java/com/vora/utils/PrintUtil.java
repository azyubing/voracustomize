package com.vora.utils;

import java.util.Collection;

public class PrintUtil {

	public static <T> void print(Collection<T> c) {
		for(T t : c) {
			System.out.println(t.toString());
		}
	}
}
