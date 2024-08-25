package com.mybank.transactionbatchprocessor.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FileMovingTasklet implements Tasklet {

	private static final Logger logger = LoggerFactory.getLogger(FileMovingTasklet.class);

	private final String inputDir;
	private final String completedDir;
	private final String errorDir;

	public FileMovingTasklet(String inputDir, String completedDir, String errorDir) {
		this.inputDir = inputDir;
		this.completedDir = completedDir;
		this.errorDir = errorDir;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws IOException {
		logger.info("Starting to move files from '{}' to '{}' or '{}'", inputDir, completedDir, errorDir);

		// Ensure the completed and error directories exist
		createDirectoryIfNotExists(completedDir);
		createDirectoryIfNotExists(errorDir);

		File folder = new File(inputDir);
		File[] files = folder.listFiles((dir, name) -> name.endsWith(".txt"));

		if (files == null || files.length == 0) {
			logger.warn("No files found in directory: {}", inputDir);
			return RepeatStatus.FINISHED;
		}

		// Check for the error flag file
		File errorFlag = new File(inputDir + "/processing_error.flag");
		boolean hasErrors = errorFlag.exists();
		logger.info("hasErrors: {}", hasErrors);

		for (File file : files) {
			// Generate the timestamp
			String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

			// Determine target directory and file name
			Path targetPath;
			if (hasErrors) {
				String newFileName = file.getName().replace(".txt", "_ERROR_" + timestamp + ".txt");
				targetPath = Paths.get(errorDir, newFileName);
				logger.info("File '{}' contains errors. Moving to '{}'", file.getName(), targetPath);
			} else {
				String newFileName = file.getName().replace(".txt", "_" + timestamp + ".txt");
				targetPath = Paths.get(completedDir, newFileName);
				logger.info("Successfully processed file: '{}'. Moving to '{}'", file.getName(), targetPath);
			}

			try {
				Files.move(file.toPath(), targetPath);
			} catch (IOException e) {
				logger.error("Failed to move file: '{}' to '{}'. Error: {}", file.getName(), targetPath, e.getMessage());
				throw e;
			}
		}

		// Clean up the error flag file after processing
		if (hasErrors) {
			if (errorFlag.delete()) {
				logger.info("Deleted error flag file: {}", errorFlag.getAbsolutePath());
			} else {
				logger.warn("Failed to delete error flag file: {}", errorFlag.getAbsolutePath());
			}
		}

		logger.info("Completed moving files from '{}' to '{}' or '{}'", inputDir, completedDir, errorDir);
		return RepeatStatus.FINISHED;
	}

	private void createDirectoryIfNotExists(String dirPath) {
		Path path = Paths.get(dirPath);
		if (!Files.exists(path)) {
			try {
				Files.createDirectories(path);
				logger.info("Created directory: {}", dirPath);
			} catch (IOException e) {
				logger.error("Failed to create directory: {}", dirPath, e.getMessage());
				throw new RuntimeException("Failed to create directory: " + dirPath, e);
			}
		}
	}
}
