package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Gatherer;
import java.util.stream.Stream;

public class TransactionStreamAnalyzer {

    public Optional<Transaction> findFirstHighValueTransaction(Stream<Transaction> stream, BigDecimal threshold) {
        requireStream(stream);
        requireNonNull(threshold, "threshold");

        return stream
                .filter(transaction -> amountOf(transaction)
                        .filter(amount -> amount.compareTo(threshold) > 0)
                        .isPresent())
                .findFirst();
    }

    public List<Transaction> takeTransactionsUntilBudgetExceeded(Stream<Transaction> stream, BigDecimal maxBudget) {
        requireStream(stream);
        requireNonNull(maxBudget, "maxBudget");

        return stream.sequential()
                .filter(transaction -> amountOf(transaction).isPresent())
                .takeWhile(withinBudget(maxBudget))
                .toList();
    }

    public List<List<Transaction>> processBatches(Stream<Transaction> stream, int batchSize, int maxBatches) {
        requireStream(stream);
        requirePositive(batchSize, "batchSize");
        requirePositive(maxBatches, "maxBatches");

        return stream
                .filter(Objects::nonNull)
                .gather(fixedBatches(batchSize, maxBatches))
                .toList();
    }

    private static Predicate<Transaction> withinBudget(BigDecimal maxBudget) {
        BigDecimal[] spent = {BigDecimal.ZERO};

        return transaction -> {
            spent[0] = spent[0].add(transaction.amount());
            return spent[0].compareTo(maxBudget) <= 0;
        };
    }

    private static <T> Gatherer<T, ?, List<T>> fixedBatches(int batchSize, int maxBatches) {
        class State {
            private List<T> batch = new ArrayList<>(batchSize);
            private int emitted;
        }

        return Gatherer.<T, State, List<T>>ofSequential(
                State::new,
                Gatherer.Integrator.of((state, element, downstream) -> {
                    state.batch.add(element);
                    if (state.batch.size() < batchSize) {
                        return true;
                    }

                    List<T> full = List.copyOf(state.batch);
                    state.batch = new ArrayList<>(batchSize);
                    state.emitted++;

                    return downstream.push(full) && state.emitted < maxBatches;
                }),
                (state, downstream) -> {
                    if (!state.batch.isEmpty() && state.emitted < maxBatches && !downstream.isRejecting()) {
                        downstream.push(List.copyOf(state.batch));
                    }
                });
    }

    private static Optional<BigDecimal> amountOf(Transaction transaction) {
        return Optional.ofNullable(transaction).map(Transaction::amount);
    }

    private static void requireStream(Stream<Transaction> stream) {
        requireNonNull(stream, "stream");
    }

    private static void requireNonNull(Object argument, String name) {
        if (argument == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }

    private static void requirePositive(int argument, String name) {
        if (argument <= 0) {
            throw new IllegalArgumentException(name + " must be greater than 0");
        }
    }
}
