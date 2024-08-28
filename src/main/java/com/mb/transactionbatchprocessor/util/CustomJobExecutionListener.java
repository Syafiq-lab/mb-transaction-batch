package com.mb.transactionbatchprocessor.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.stereotype.Component;

@Component
public class CustomJobExecutionListener implements JobExecutionListener {

	private static final Logger logger = LoggerFactory.getLogger(CustomJobExecutionListener.class);

	@Override
	public void beforeJob(JobExecution jobExecution) {
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		long successCount = jobExecution.getStepExecutions().stream()
				.mapToLong(StepExecution::getWriteCount)
				.sum();

		long failureCount = jobExecution.getStepExecutions().stream()
				.mapToLong(stepExecution -> stepExecution.getProcessSkipCount() + stepExecution.getWriteSkipCount())
				.sum();

		logger.info("Job completed with the following results:");
		logger.info("Successfully processed records: {}", successCount);
		logger.info("Failed records: {}", failureCount);
	}
}
