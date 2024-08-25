package com.mybank.transactionbatchprocessor;

import com.mybank.transactionbatchprocessor.batch.FileMovingTasklet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@SpringBootTest
class FileMovingTaskletTests {

	private static final String TEST_INPUT_DIR = "test/resources/input";
	private static final String TEST_COMPLETED_DIR = "test/resources/completed";
	private static final String TEST_ERROR_DIR = "test/resources/error";

	private FileMovingTasklet fileMovingTasklet;

	@BeforeEach
	void setUp() throws IOException {
		Files.createDirectories(Paths.get(TEST_INPUT_DIR));
		Files.createDirectories(Paths.get(TEST_COMPLETED_DIR));
		fileMovingTasklet = new FileMovingTasklet(TEST_INPUT_DIR, TEST_COMPLETED_DIR,TEST_ERROR_DIR);
	}

	@AfterEach
	void tearDown() throws IOException {
		// Only delete files in the completed directory, leave the input directory intact.
		Files.walk(Paths.get(TEST_COMPLETED_DIR))
				.map(Path::toFile)
				.forEach(File::delete);
	}

	private Path getCompletedFilePath(String originalFileName) {
		String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		String newFileName = originalFileName.replace(".txt", "_" + timestamp + ".txt");
		return Paths.get(TEST_COMPLETED_DIR, newFileName);
	}

	@Test
	void testEmptyFile() throws Exception {
		String originalFileName = "testEmptyFile.txt";
		Path inputFile = Paths.get(TEST_INPUT_DIR, originalFileName);
		Files.createFile(inputFile);

		StepContribution contribution = mock(StepContribution.class);
		ChunkContext chunkContext = mock(ChunkContext.class);

		RepeatStatus status = fileMovingTasklet.execute(contribution, chunkContext);

		assertTrue(status == RepeatStatus.FINISHED);
		assertFalse(Files.exists(inputFile));

		// Since we're dealing with a timestamped file, check for the presence of any file with the timestamped pattern
		Path completedFile = getCompletedFilePath(originalFileName);
		assertTrue(Files.exists(completedFile));

		Files.deleteIfExists(completedFile);
	}

	@Test
	void testFileWithIncorrectFormat() throws Exception {
		String originalFileName = "testIncorrectFormatFile.txt";
		Path inputFile = Paths.get(TEST_INPUT_DIR, originalFileName);
		Files.write(inputFile, "IncorrectFormatData".getBytes());

		StepContribution contribution = mock(StepContribution.class);
		ChunkContext chunkContext = mock(ChunkContext.class);

		RepeatStatus status = fileMovingTasklet.execute(contribution, chunkContext);

		assertTrue(status == RepeatStatus.FINISHED);
		assertFalse(Files.exists(inputFile));

		Path completedFile = getCompletedFilePath(originalFileName);
		assertTrue(Files.exists(completedFile));

		Files.deleteIfExists(completedFile);
	}

	@Test
	void testSuccessfulJobExecution() throws Exception {
		String originalFileName = "testCorrectFile.txt";
		Path inputFile = Paths.get(TEST_INPUT_DIR, originalFileName);
		Files.write(inputFile, "123456|123.45|Description|2023-08-24|12:34:56|789".getBytes());

		StepContribution contribution = mock(StepContribution.class);
		ChunkContext chunkContext = mock(ChunkContext.class);

		RepeatStatus status = fileMovingTasklet.execute(contribution, chunkContext);

		assertTrue(status == RepeatStatus.FINISHED);
		assertFalse(Files.exists(inputFile));

		Path completedFile = getCompletedFilePath(originalFileName);
		assertTrue(Files.exists(completedFile));

		Files.deleteIfExists(completedFile);
	}

	@Test
	void testFileMovementFailure() throws Exception {
		String originalFileName = "testFileMovementFailure.txt";
		Path inputFile = Paths.get(TEST_INPUT_DIR, originalFileName);
		Files.createFile(inputFile);

		// Lock or set the completed directory as read-only to simulate failure
		File completedDirFile = Paths.get(TEST_COMPLETED_DIR).toFile();
		completedDirFile.setWritable(false);

		StepContribution contribution = mock(StepContribution.class);
		ChunkContext chunkContext = mock(ChunkContext.class);

		try {
			fileMovingTasklet.execute(contribution, chunkContext);
		} catch (IOException e) {
			assertTrue(Files.exists(inputFile));
			assertFalse(Files.exists(getCompletedFilePath(originalFileName)));
		} finally {
			completedDirFile.setWritable(true); // Reset directory permissions
			Files.deleteIfExists(inputFile);
		}
	}
}
