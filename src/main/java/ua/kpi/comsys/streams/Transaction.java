package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record Transaction(String transactionId, String category, BigDecimal amount, LocalDateTime timestamp) {}
