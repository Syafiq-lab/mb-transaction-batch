package com.mybank.transactionbatchprocessor;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class TransactionBatchProcessorApplication implements CommandLineRunner {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job importTransactionRecordJob;

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(TransactionBatchProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);  // Disable web environment if not needed
		ConfigurableApplicationContext context = app.run(args);

		// Exit the application context and close the application
		int exitCode = SpringApplication.exit(context);
		System.exit(exitCode);
	}

	@Override
	public void run(String... args) throws Exception {
		// Trigger the job execution
		JobParameters jobParameters = new JobParametersBuilder()
				.addLong("startAt", System.currentTimeMillis())
				.toJobParameters();
		JobExecution execution = jobLauncher.run(importTransactionRecordJob, jobParameters);
		System.out.println("Job Status: " + execution.getStatus());
	}
}
