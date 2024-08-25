package com.mybank.transactionbatchprocessor.util;

import org.springframework.core.convert.converter.Converter;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class StringToLocalTimeConverter implements Converter<String, LocalTime> {
	@Override
	public LocalTime convert(String source) {
		return LocalTime.parse(source, DateTimeFormatter.ISO_LOCAL_TIME);
	}
}
