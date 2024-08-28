package com.mb.transactionbatchprocessor.util;

import org.springframework.core.convert.converter.Converter;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class StringToLocalDateConverter implements Converter<String, LocalDate> {
	@Override
	public LocalDate convert(String source) {
		return LocalDate.parse(source, DateTimeFormatter.ISO_LOCAL_DATE);
	}
}

