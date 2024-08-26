package com.mybank.transactionbatchprocessor.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalTime;

@Entity
@Data
@NoArgsConstructor
public class TransactionRecord {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private String accountNumber;
	private Double trxAmount;
	private String description;
	private LocalDate trxDate;
	private LocalTime trxTime;
	private String customerId;

	@Version
	private Integer version;

	@PrePersist
	@PreUpdate
	protected void onCreate() {
		if (version == null) {
			version = 0; // Initialize version to 0
		}
	}
}
