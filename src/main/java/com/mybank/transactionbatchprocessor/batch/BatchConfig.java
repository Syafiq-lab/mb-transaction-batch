package com.mybank.transactionbatchprocessor.batch;

import com.mybank.transactionbatchprocessor.exception.InvalidTransactionRecordException;
import com.mybank.transactionbatchprocessor.model.TransactionRecord;
import com.mybank.transactionbatchprocessor.util.StringToLocalDateConverter;
import com.mybank.transactionbatchprocessor.util.StringToLocalTimeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

	private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);

	@Value("${transaction.input.dir}")
	private String inputDir;

	@Value("${transaction.completed.dir}")
	private String completedDir;

	@Value("${transaction.error.dir}")
	private String errorDir;

	private final DataSource dataSource;
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;

	public BatchConfig(DataSource dataSource, JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		this.dataSource = dataSource;
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
	}

	@Bean
	public MultiResourceItemReader<TransactionRecord> multiResourceItemReader() {
		logger.info("Loading resources from directory: {}", inputDir);
		Resource[] resources = getResources(inputDir);

		logger.info("Configuring MultiResourceItemReader with {} resources", resources.length);
		return new MultiResourceItemReaderBuilder<TransactionRecord>()
				.name("multiResourceItemReader")
				.resources(resources)
				.delegate(transactionRecordReader())
				.build();
	}

	@Bean
	public FlatFileItemReader<TransactionRecord> transactionRecordReader() {
		BeanWrapperFieldSetMapper<TransactionRecord> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
		fieldSetMapper.setTargetType(TransactionRecord.class);

		DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();
		conversionService.addConverter(new StringToLocalDateConverter());
		conversionService.addConverter(new StringToLocalTimeConverter());
		fieldSetMapper.setConversionService(conversionService);

		return new FlatFileItemReaderBuilder<TransactionRecord>()
				.name("transactionRecordItemReader")
				.linesToSkip(1)  // Skip the header row
				.delimited()
				.delimiter("|")
				.names("accountNumber", "trxAmount", "description", "trxDate", "trxTime", "customerId")
				.fieldSetMapper(fieldSetMapper)
				.build();
	}

	@Bean
	public ItemProcessor<TransactionRecord, TransactionRecord> transactionRecordProcessor() {
		logger.info("Configuring ItemProcessor for TransactionRecord");
		return transactionRecord -> {
			// Perform validation
			try {
				Double trxAmount = transactionRecord.getTrxAmount();
				if (transactionRecord.getAccountNumber() == null || trxAmount == null || trxAmount.isNaN()) {
					throw new InvalidTransactionRecordException("Invalid data in record: " + transactionRecord);
				}
			} catch (NumberFormatException e) {
				throw new InvalidTransactionRecordException("Invalid number format in record: " + transactionRecord);
			}

			// Initialize version if null
			if (transactionRecord.getVersion() == null) {
				transactionRecord.setVersion(0);  // Set the initial version to 0
			}

			return transactionRecord;
		};
	}

	@Bean
	public ItemWriter<TransactionRecord> transactionRecordWriter() {
		logger.info("Configuring JdbcBatchItemWriter for TransactionRecord");
		return new JdbcBatchItemWriterBuilder<TransactionRecord>()
				.dataSource(dataSource)
				.beanMapped()
				.sql("INSERT INTO transaction_record (account_number, trx_amount, description, trx_date, trx_time, customer_id, version) " +
						"VALUES (:accountNumber, :trxAmount, :description, :trxDate, :trxTime, :customerId, :version)")
				.build();
	}

	@Bean
	public Job importTransactionRecordJob(JobCompletionNotificationListener listener, Step step1, Step moveFilesStep) {
		logger.info("Building importTransactionRecordJob");
		return new JobBuilder("importTransactionRecordJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.start(step1)
				.next(moveFilesStep)
				.build();
	}

	@Bean
	public Step step1() {
		logger.info("Configuring Step1: Reading, Processing, and Writing TransactionRecords");
		return new StepBuilder("step1", jobRepository)
				.<TransactionRecord, TransactionRecord>chunk(10, transactionManager)
				.reader(multiResourceItemReader())
				.processor(transactionRecordProcessor())
				.writer(transactionRecordWriter())
				.faultTolerant()
				.skip(InvalidTransactionRecordException.class)
				.skip(FlatFileParseException.class)  // Skip parsing errors
				.skipLimit(Integer.MAX_VALUE)
				.listener(skipListener())
				.build();
	}

	@Bean
	public Step moveFilesStep() {
		logger.info("Configuring moveFilesStep to move processed files");
		return new StepBuilder("moveFilesStep", jobRepository)
				.tasklet(new FileMovingTasklet(inputDir, completedDir, errorDir), transactionManager)
				.build();
	}

	@Bean
	public SkipListener<TransactionRecord, TransactionRecord> skipListener() {
		return new SkipListener<>() {
			@Override
			public void onSkipInRead(Throwable t) {
				logger.error("Error during reading, skipped: {}", t.getMessage());
				markFileAsError();
			}

			@Override
			public void onSkipInWrite(TransactionRecord item, Throwable t) {
				logger.error("Error during writing, skipped item: {}, error: {}", item, t.getMessage());
				markFileAsError();
			}

			@Override
			public void onSkipInProcess(TransactionRecord item, Throwable t) {
				logger.error("Error during processing, skipped item: {}, error: {}", item, t.getMessage());
				markFileAsError();
			}

			private void markFileAsError() {
				try {
					File errorFile = new File(inputDir + "/processing_error.flag");
					if (!errorFile.exists()) {
						errorFile.createNewFile();
					}
					logger.info("Created error flag file: {}", errorFile.getAbsolutePath());
				} catch (IOException e) {
					logger.error("Failed to create error flag file. Error: {}", e.getMessage());
				}
			}
		};
	}

	private Resource[] getResources(String directoryPath) {
		logger.info("Fetching files from directory: {}", directoryPath);
		File folder = new File(directoryPath);
		File[] files = folder.listFiles((dir, name) -> name.endsWith(".txt"));

		if (files == null || files.length == 0) {
			logger.warn("No .txt files found in directory: {}", directoryPath);
			return new Resource[0];
		}

		logger.info("Found {} files in directory: {}", files.length, directoryPath);
		return Arrays.stream(files)
				.map(FileSystemResource::new)
				.toArray(Resource[]::new);
	}

}
