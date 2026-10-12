package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

public class Main {

    private static final List<String> CATEGORIES = List.of("books", "electronics", "grocery");

    private static final LocalDateTime START = LocalDateTime.of(2025, 5, 1, 9, 0);

    public static void main(String[] args) {
        TransactionStreamAnalyzer analyzer = new TransactionStreamAnalyzer();

        String firstHighValue = analyzer.findFirstHighValueTransaction(transactions(), new BigDecimal("450"))
                .map(Main::describe)
                .orElse("not found");
        System.out.println("First transaction above 450: " + firstHighValue);

        List<String> withinBudget = analyzer.takeTransactionsUntilBudgetExceeded(transactions(), new BigDecimal("1000"))
                .stream()
                .map(Main::describe)
                .toList();
        System.out.println("Within budget 1000: " + withinBudget);

        List<List<String>> batches = analyzer.processBatches(transactions(), 3, 2).stream()
                .map(batch -> batch.stream().map(Transaction::transactionId).toList())
                .toList();
        System.out.println("2 batches of 3: " + batches);
    }

    private static Stream<Transaction> transactions() {
        return Stream.iterate(1, number -> number + 1)
                .map(number -> new Transaction(
                        "tx-" + number,
                        CATEGORIES.get(number % CATEGORIES.size()),
                        BigDecimal.valueOf(number * 100L),
                        START.plusMinutes(number)));
    }

    private static String describe(Transaction transaction) {
        return transaction.transactionId() + " (" + transaction.amount() + ")";
    }
}
