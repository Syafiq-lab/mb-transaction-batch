package com.mb.transactionbatchprocessor.aspect;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
@Aspect
public class LoggingAspect {

	private static final Logger logger = LoggerFactory.getLogger(LoggingAspect.class);

	@Pointcut("execution(* com.mb.transactionbatchprocessor..*(..)) && !execution(* com.mb.transactionbatchprocessor.batch.BatchConfig.transactionRecordProcessor(..)) && !execution(* com.mb.transactionbatchprocessor.batch.BatchConfig.process(..))")
	public void applicationPackagePointcut() {
	}

	@Before("applicationPackagePointcut()")
	public void logBefore(JoinPoint joinPoint) {
		logger.info("Entering method: {} with arguments: {}",
				joinPoint.getSignature().getName(),
				Arrays.toString(joinPoint.getArgs()));
	}

	@AfterReturning(pointcut = "applicationPackagePointcut()", returning = "result")
	public void logAfterReturning(JoinPoint joinPoint, Object result) {
		logger.info("Method {} returned: {}",
				joinPoint.getSignature().getName(),
				result);
	}
}
