package com.mybank.transactionbatchprocessor.exception;

public class InvalidTransactionRecordException extends RuntimeException {
	public InvalidTransactionRecordException(String message) {
		super(message);
	}
}
