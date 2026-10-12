package ua.kpi.comsys.streams;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(2)
class TransactionStreamAnalyzerTest {

    private static final LocalDateTime START = LocalDateTime.of(2025, 5, 1, 9, 0);

    private final TransactionStreamAnalyzer analyzer = new TransactionStreamAnalyzer();

    private static Transaction tx(int number, String amount) {
        return new Transaction("tx-" + number, "books", new BigDecimal(amount), START.plusMinutes(number));
    }

    private static Stream<Transaction> growing() {
        return Stream.iterate(1, number -> number + 1)
                .map(number -> tx(number, String.valueOf(number * 100)));
    }

    private static Stream<Transaction> constant(String amount, AtomicInteger counter) {
        return Stream.generate(() -> tx(counter.incrementAndGet(), amount));
    }

    private static List<String> ids(List<Transaction> transactions) {
        return transactions.stream().map(Transaction::transactionId).toList();
    }

    @Nested
    class FindFirstHighValueTransaction {

        @Test
        @DisplayName("null замість стріма -> IllegalArgumentException")
        void nullStreamIsRejected() {
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> analyzer.findFirstHighValueTransaction(null, BigDecimal.TEN));

            assertEquals("stream cannot be null", thrown.getMessage());
        }

        @Test
        @DisplayName("null замість ліміту -> IllegalArgumentException")
        void nullThresholdIsRejected() {
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> analyzer.findFirstHighValueTransaction(Stream.empty(), null));

            assertEquals("threshold cannot be null", thrown.getMessage());
        }

        @Test
        @DisplayName("порожній стрім -> Optional.empty()")
        void emptyStreamGivesEmptyOptional() {
            assertTrue(analyzer.findFirstHighValueTransaction(Stream.empty(), BigDecimal.TEN).isEmpty());
        }

        @Test
        @DisplayName("у скінченному стрімі немає суми понад ліміт -> Optional.empty()")
        void noMatchInFiniteStreamGivesEmptyOptional() {
            Stream<Transaction> stream = Stream.of(tx(1, "10"), tx(2, "20"), tx(3, "30"));

            assertEquals(Optional.empty(), analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100")));
        }

        @Test
        @DisplayName("зі скінченного стріма повертається перша транзакція понад ліміт, а не найбільша")
        void returnsFirstMatchNotTheLargest() {
            Stream<Transaction> stream = Stream.of(tx(1, "50"), tx(2, "150"), tx(3, "900"));

            String id = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100"))
                    .map(Transaction::transactionId)
                    .orElse("none");

            assertEquals("tx-2", id);
        }

        @Test
        @DisplayName("сума, що дорівнює ліміту, не вважається перевищенням")
        void amountEqualToThresholdIsNotHighValue() {
            Stream<Transaction> stream = Stream.of(tx(1, "100"), tx(2, "100.00"), tx(3, "100.01"));

            String id = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100"))
                    .map(Transaction::transactionId)
                    .orElse("none");

            assertEquals("tx-3", id);
        }

        @Test
        @DisplayName("null-елементи і транзакції без суми пропускаються")
        void nullElementsAndNullAmountsAreSkipped() {
            Stream<Transaction> stream = Stream.of(
                    null,
                    new Transaction("no-amount", "books", null, START),
                    tx(3, "500"));

            String id = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100"))
                    .map(Transaction::transactionId)
                    .orElse("none");

            assertEquals("tx-3", id);
        }

        @Test
        @DisplayName("нескінченний Stream.iterate: пошук зупиняється на першому збігу")
        void infiniteIterateStreamDoesNotHang() {
            Optional<Transaction> found = analyzer.findFirstHighValueTransaction(growing(), new BigDecimal("450"));

            assertEquals(Optional.of(tx(5, "500")), found);
        }

        @Test
        @DisplayName("нескінченний Stream.generate: після збігу джерело більше не читається")
        void infiniteGenerateStreamIsReadLazily() {
            AtomicInteger generated = new AtomicInteger();
            Stream<Transaction> stream = Stream.generate(() -> {
                int number = generated.incrementAndGet();
                return tx(number, number == 7 ? "1000" : "1");
            });

            String id = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("999"))
                    .map(Transaction::transactionId)
                    .orElse("none");

            assertEquals("tx-7", id);
            assertEquals(7, generated.get());
        }
    }

    @Nested
    class TakeTransactionsUntilBudgetExceeded {

        @Test
        @DisplayName("null замість стріма -> IllegalArgumentException")
        void nullStreamIsRejected() {
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> analyzer.takeTransactionsUntilBudgetExceeded(null, BigDecimal.TEN));

            assertEquals("stream cannot be null", thrown.getMessage());
        }

        @Test
        @DisplayName("null замість бюджету -> IllegalArgumentException")
        void nullBudgetIsRejected() {
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> analyzer.takeTransactionsUntilBudgetExceeded(Stream.empty(), null));

            assertEquals("maxBudget cannot be null", thrown.getMessage());
        }

        @Test
        @DisplayName("порожній стрім -> порожній список")
        void emptyStreamGivesEmptyList() {
            assertTrue(analyzer.takeTransactionsUntilBudgetExceeded(Stream.empty(), BigDecimal.TEN).isEmpty());
        }

        @Test
        @DisplayName("скінченний стрім, що цілком вкладається в бюджет, повертається повністю")
        void wholeFiniteStreamFitsIntoBudget() {
            Stream<Transaction> stream = Stream.of(tx(1, "10"), tx(2, "20"), tx(3, "30"));

            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("1000"));

            assertEquals(List.of("tx-1", "tx-2", "tx-3"), ids(taken));
        }

        @Test
        @DisplayName("транзакція, яка виводить суму за бюджет, у результат не потрапляє")
        void transactionThatExceedsBudgetIsExcluded() {
            Stream<Transaction> stream = Stream.of(tx(1, "40"), tx(2, "40"), tx(3, "40"), tx(4, "1"));

            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100"));

            assertEquals(List.of("tx-1", "tx-2"), ids(taken));
        }

        @Test
        @DisplayName("сума рівно в бюджет ще не є перевищенням")
        void sumEqualToBudgetIsStillAllowed() {
            Stream<Transaction> stream = Stream.of(tx(1, "60"), tx(2, "40"), tx(3, "0.01"));

            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100.00"));

            assertEquals(List.of("tx-1", "tx-2"), ids(taken));
        }

        @Test
        @DisplayName("перша ж транзакція більша за бюджет -> порожній список")
        void firstTransactionAboveBudgetGivesEmptyList() {
            Stream<Transaction> stream = Stream.of(tx(1, "500"), tx(2, "1"));

            assertTrue(analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100")).isEmpty());
        }

        @Test
        @DisplayName("null-елементи і транзакції без суми пропускаються")
        void nullElementsAndNullAmountsAreSkipped() {
            Stream<Transaction> stream = Stream.of(
                    tx(1, "10"),
                    null,
                    new Transaction("no-amount", "books", null, START),
                    tx(4, "20"));

            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100"));

            assertEquals(List.of("tx-1", "tx-4"), ids(taken));
        }

        @Test
        @DisplayName("нескінченний Stream.iterate: 100 + 200 + 300 + 400 вкладається в 1000")
        void infiniteIterateStreamDoesNotHang() {
            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(growing(), new BigDecimal("1000"));

            assertEquals(List.of("tx-1", "tx-2", "tx-3", "tx-4"), ids(taken));
        }

        @Test
        @DisplayName("нескінченний Stream.generate: читається лише один зайвий елемент")
        void infiniteGenerateStreamIsReadLazily() {
            AtomicInteger generated = new AtomicInteger();

            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(
                    constant("25", generated), new BigDecimal("110"));

            assertEquals(4, taken.size());
            assertEquals(5, generated.get());
        }
    }

    @Nested
    class ProcessBatches {

        @Test
        @DisplayName("null замість стріма -> IllegalArgumentException")
        void nullStreamIsRejected() {
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> analyzer.processBatches(null, 2, 2));

            assertEquals("stream cannot be null", thrown.getMessage());
        }

        @ParameterizedTest
        @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
        @DisplayName("batchSize <= 0 -> IllegalArgumentException")
        void nonPositiveBatchSizeIsRejected(int batchSize) {
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> analyzer.processBatches(growing(), batchSize, 2));

            assertEquals("batchSize must be greater than 0", thrown.getMessage());
        }

        @ParameterizedTest
        @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
        @DisplayName("maxBatches <= 0 -> IllegalArgumentException")
        void nonPositiveMaxBatchesIsRejected(int maxBatches) {
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> analyzer.processBatches(growing(), 2, maxBatches));

            assertEquals("maxBatches must be greater than 0", thrown.getMessage());
        }

        @Test
        @DisplayName("порожній стрім -> жодного батча")
        void emptyStreamGivesNoBatches() {
            assertTrue(analyzer.processBatches(Stream.empty(), 3, 5).isEmpty());
        }

        @Test
        @DisplayName("скінченний стрім ділиться на вікна без залишку")
        void finiteStreamIsSplitIntoFullBatches() {
            Stream<Transaction> stream = Stream.of(tx(1, "1"), tx(2, "1"), tx(3, "1"), tx(4, "1"));

            List<List<Transaction>> batches = analyzer.processBatches(stream, 2, 10);

            assertEquals(List.of(List.of("tx-1", "tx-2"), List.of("tx-3", "tx-4")),
                    batches.stream().map(TransactionStreamAnalyzerTest::ids).toList());
        }

        @Test
        @DisplayName("неповний залишок скінченного стріма стає останнім батчем")
        void trailingPartialBatchIsKept() {
            Stream<Transaction> stream = Stream.of(tx(1, "1"), tx(2, "1"), tx(3, "1"));

            List<List<Transaction>> batches = analyzer.processBatches(stream, 2, 10);

            assertEquals(List.of(List.of("tx-1", "tx-2"), List.of("tx-3")),
                    batches.stream().map(TransactionStreamAnalyzerTest::ids).toList());
        }

        @Test
        @DisplayName("після maxBatches повних батчів залишок відкидається")
        void remainderAfterMaxBatchesIsDropped() {
            Stream<Transaction> stream = Stream.of(tx(1, "1"), tx(2, "1"), tx(3, "1"), tx(4, "1"), tx(5, "1"));

            List<List<Transaction>> batches = analyzer.processBatches(stream, 2, 2);

            assertEquals(List.of(List.of("tx-1", "tx-2"), List.of("tx-3", "tx-4")),
                    batches.stream().map(TransactionStreamAnalyzerTest::ids).toList());
        }

        @Test
        @DisplayName("null-елементи в батчі не потрапляють")
        void nullElementsAreSkipped() {
            Stream<Transaction> stream = Stream.of(tx(1, "1"), null, tx(3, "1"));

            List<List<Transaction>> batches = analyzer.processBatches(stream, 2, 10);

            assertEquals(List.of(List.of("tx-1", "tx-3")),
                    batches.stream().map(TransactionStreamAnalyzerTest::ids).toList());
        }

        @Test
        @DisplayName("батчі повертаються незмінними списками")
        void batchesAreImmutable() {
            List<List<Transaction>> batches = analyzer.processBatches(growing(), 2, 1);

            assertThrows(UnsupportedOperationException.class, () -> batches.getFirst().add(tx(99, "1")));
        }

        @Test
        @DisplayName("нескінченний Stream.iterate: рівно maxBatches батчів по batchSize елементів")
        void infiniteIterateStreamGivesExactlyMaxBatches() {
            List<List<Transaction>> batches = analyzer.processBatches(growing(), 3, 2);

            assertEquals(List.of(List.of("tx-1", "tx-2", "tx-3"), List.of("tx-4", "tx-5", "tx-6")),
                    batches.stream().map(TransactionStreamAnalyzerTest::ids).toList());
        }

        @Test
        @DisplayName("нескінченний Stream.generate: з джерела взято рівно batchSize * maxBatches елементів")
        void infiniteGenerateStreamIsReadLazily() {
            AtomicInteger generated = new AtomicInteger();

            List<List<Transaction>> batches = analyzer.processBatches(constant("1", generated), 4, 25);

            assertEquals(25, batches.size());
            assertTrue(batches.stream().allMatch(batch -> batch.size() == 4));
            assertEquals(100, generated.get());
        }

        @Test
        @DisplayName("batchSize = 1 і maxBatches = 1 -> один батч з одним елементом")
        void singleElementBatch() {
            List<List<Transaction>> batches = analyzer.processBatches(growing(), 1, 1);

            assertEquals(List.of(List.of(tx(1, "100"))), batches);
        }
    }
}
