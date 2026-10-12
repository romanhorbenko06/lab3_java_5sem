# Лабораторна робота №3 - нескінченні стріми, Optional та Stream Gatherers

## Про що проєкт

Сервіс аналітики транзакцій інтернет-магазину. На вхід він отримує
`Stream<Transaction>`, який може бути нескінченним, і читає з нього рівно
стільки елементів, скільки потрібно для відповіді.

Клас `TransactionStreamAnalyzer` має три методи:

- `findFirstHighValueTransaction(stream, threshold)` - перша транзакція із сумою
  понад `threshold`, результат загорнутий в `Optional`;
- `takeTransactionsUntilBudgetExceeded(stream, maxBudget)` - транзакції з початку
  стріма, поки їхня сума не перевищила `maxBudget`;
- `processBatches(stream, batchSize, maxBatches)` - стрім, порізаний на вікна по
  `batchSize` елементів, не більше ніж `maxBatches` вікон. Тут працює власний
  `Gatherer`.

Модель - record `Transaction(transactionId, category, amount, timestamp)`.
Сторонніх бібліотек немає, усе лежить у пакеті `ua.kpi.comsys.streams`.

## Розрахунок варіанта

Залікової книжки немає, тому беремо порядковий номер у списку групи: **N = 6**.

```
V = N mod 3 = 6 mod 3 = 0
```

**Варіант 0 - електронна комерція та стрім транзакцій.**

## Поведінка методів

Спільне для всіх трьох: `null` замість стріма чи іншого обов'язкового аргументу
дає `IllegalArgumentException`. Стрім перевіряється першим. `null`-елементи
всередині стріма пропускаються.

### findFirstHighValueTransaction

- `stream == null` -> `IllegalArgumentException("stream cannot be null")`;
- `threshold == null` -> `IllegalArgumentException("threshold cannot be null")`;
- "перевищує" означає строго більше: сума, що дорівнює ліміту, не підходить;
- суми порівнюються через `compareTo`, тому `100` і `100.00` рівні;
- транзакція з `amount == null` пропускається;
- якщо потрібної транзакції немає - `Optional.empty()`.

Усередині це `filter(...).findFirst()`. `findFirst` - операція короткого
замикання, тому після першого збігу джерело більше не читається.

Є одне обмеження, яке кодом не обійти: якщо стрім нескінченний і потрібної
транзакції в ньому немає взагалі, пошук не закінчиться. `Optional.empty()`
можна отримати лише зі скінченного стріма.

### takeTransactionsUntilBudgetExceeded

- `stream == null` -> `IllegalArgumentException("stream cannot be null")`;
- `maxBudget == null` -> `IllegalArgumentException("maxBudget cannot be null")`;
- транзакції беруться з початку стріма, поки накопичена сума `<= maxBudget`;
- транзакція, на якій сума вперше стала більшою за бюджет, у список не
  потрапляє, і на ній обробка зупиняється;
- сума рівно в бюджет ще дозволена;
- якщо перша ж транзакція більша за бюджет - порожній список;
- транзакція з `amount == null` пропускається і на суму не впливає.

Зупинку робить `takeWhile`. Його предикат тримає накопичену суму, тобто має
стан, тому перед ним стоїть `sequential()`: у паралельному стрімі такий
предикат рахував би неправильно.

### processBatches

- `stream == null` -> `IllegalArgumentException("stream cannot be null")`;
- `batchSize <= 0` -> `IllegalArgumentException("batchSize must be greater than 0")`;
- `maxBatches <= 0` -> `IllegalArgumentException("maxBatches must be greater than 0")`;
- елементи групуються у вікна по `batchSize` у порядку надходження;
- після `maxBatches` повних вікон обробка зупиняється, решта стріма не читається;
- якщо скінченний стрім закінчився посеред вікна, а ліміт вікон ще не
  вичерпано, неповний залишок стає останнім батчем (так само поводиться
  стандартний `Gatherers.windowFixed`);
- кожен батч - незмінний список.

## Кастомний Gatherer

Метод `fixedBatches(batchSize, maxBatches)` збирає `Gatherer` через
`Gatherer.ofSequential` із трьох частин:

| Частина      | Що робить                                                                 |
|--------------|---------------------------------------------------------------------------|
| initializer  | створює стан: поточне вікно і лічильник уже виданих батчів                |
| integrator   | додає елемент у вікно; коли вікно заповнилось, віддає його далі           |
| finisher     | наприкінці скінченного стріма віддає неповний залишок, якщо ліміт дозволяє |

Від зависання рятує значення, яке повертає integrator. Поки батчів менше за
`maxBatches`, він повертає `true` і стрім подає наступний елемент. Після
останнього батча повертає `false`, і джерело більше не опитується. Тому
integrator створено через `Integrator.of`, а не `Integrator.ofGreedy`: жадібний
варіант обіцяє, що споживатиме все, і для нескінченного стріма не годиться.

Те, що читається рівно `batchSize * maxBatches` елементів, перевіряє окремий
тест із лічильником у `Stream.generate`.

## Робота з Optional

- `findFirstHighValueTransaction` повертає `Optional` прямо з `findFirst()`,
  нічого не розпаковуючи;
- сума транзакції дістається через
  `Optional.ofNullable(transaction).map(Transaction::amount)`, а умова
  перевіряється через `filter(...).isPresent()`. Так `null`-транзакція
  і `null`-сума обробляються одним ланцюжком без `if`;
- у `Main` і в тестах результат читається через `map(...).orElse(...)`;
- `get()` у проєкті не викликається ніде.

## Приклад роботи

Джерело в `Main` - нескінченний `Stream.iterate`: `tx-1` на 100, `tx-2` на 200,
`tx-3` на 300 і так далі.

Вивід:

```
First transaction above 450: tx-5 (500)
Within budget 1000: [tx-1 (100), tx-2 (200), tx-3 (300), tx-4 (400)]
2 batches of 3: [[tx-1, tx-2, tx-3], [tx-4, tx-5, tx-6]]
```

У другому рядку 100 + 200 + 300 + 400 = 1000, це рівно бюджет. `tx-5` дала б
1500, тому на ній обробка зупинилась.

## Gradle та Version Catalog

Проєкт збирається через Gradle Wrapper 9.3.1, скрипт збірки написаний на
Kotlin DSL (`build.gradle.kts`).

Версії та залежності винесені в `gradle/libs.versions.toml`:

```toml
[versions]
java = "23"
junit = "5.11.4"

[libraries]
junit-bom = { module = "org.junit:junit-bom", version.ref = "junit" }
junit-jupiter = { module = "org.junit.jupiter:junit-jupiter" }
junit-platform-launcher = { module = "org.junit.platform:junit-platform-launcher" }
```

У `build.gradle.kts` на них посилаються через `libs`:

```kotlin
dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
```

Версія Java для toolchain теж читається з каталогу
(`libs.versions.java.get()`).

Stream Gatherers у Java 23 мають статус preview (JEP 473), фінальними вони
стали в Java 24. Тому компілятор, тести і `run` отримують прапорець
`--enable-preview`. Він уже прописаний у `build.gradle.kts`, руками нічого
додавати не треба.

## Структура

```
build.gradle.kts              скрипт збірки
settings.gradle.kts           назва проєкту
gradle/libs.versions.toml     каталог версій (Java, JUnit)
gradle/wrapper/               Gradle Wrapper 9.3.1
gradlew, gradlew.bat          скрипти запуску wrapper
src/main/java/ua/kpi/comsys/streams/
    Transaction.java                  record із даними транзакції
    TransactionStreamAnalyzer.java    три методи аналізу + Gatherer
    Main.java                         демонстраційний запуск
src/test/java/ua/kpi/comsys/streams/
    TransactionStreamAnalyzerTest.java   усі три методи, винятки, нескінченні стріми
```

## Як запустити

Потрібен JDK 23. Gradle ставити не треба, його завантажить wrapper.

Збірка разом із тестами:

```bash
./gradlew build
```

Тести:

```bash
./gradlew test
```

Звіт із результатами тестів після цього лежить у
`build/reports/tests/test/index.html`.

Демонстрація в консолі:

```bash
./gradlew run
```

На Windows замість `./gradlew` пишемо `gradlew.bat`.

З IntelliJ IDEA: проєкт відкривається як Gradle-проєкт (File -> Open ->
`build.gradle.kts` -> Open as Project), далі правий клік на теці
`src/test/java` -> Run All Tests.

## Тести

35 тестів на JUnit 5 в одному класі `TransactionStreamAnalyzerTest`, поділені на
три вкладені групи. На класі стоїть `@Timeout(2)`, тобто кожен тест має дві
секунди: якби якийсь метод завис на нескінченному стрімі, тест упав би, а не
висів.

Нескінченні джерела двох видів. `Stream.iterate` дає транзакції зі зростаючою
сумою. `Stream.generate` має всередині лічильник, і за ним видно, скільки
елементів метод реально прочитав.

`FindFirstHighValueTransaction` (9 тестів):

- `null`-стрім і `null`-ліміт із точними текстами винятків;
- порожній стрім і скінченний стрім без збігу дають `Optional.empty()`;
- повертається перша транзакція понад ліміт, а не найбільша;
- сума, що дорівнює ліміту, не підходить;
- `null`-елементи і транзакції без суми пропускаються;
- нескінченний `Stream.iterate`;
- нескінченний `Stream.generate`: збіг на сьомому елементі, прочитано рівно сім.

`TakeTransactionsUntilBudgetExceeded` (10 тестів):

- `null`-стрім і `null`-бюджет;
- порожній стрім;
- скінченний стрім, що цілком вкладається в бюджет;
- транзакція, яка перевищує бюджет, у результат не потрапляє;
- сума рівно в бюджет;
- перша транзакція більша за бюджет;
- `null`-елементи і транзакції без суми;
- нескінченний `Stream.iterate`;
- нескінченний `Stream.generate`: взято чотири елементи, прочитано п'ять.

`ProcessBatches` (16 тестів):

- `null`-стрім;
- `batchSize` і `maxBatches` зі значеннями `0`, `-1`, `Integer.MIN_VALUE`;
- порожній стрім;
- поділ без залишку і з неповним останнім батчем;
- залишок після `maxBatches` батчів відкидається;
- `null`-елементи;
- батчі незмінні;
- нескінченний `Stream.iterate`: рівно два батчі по три;
- нескінченний `Stream.generate`: 25 батчів по 4, прочитано рівно 100 елементів;
- `batchSize = 1`, `maxBatches = 1`.
