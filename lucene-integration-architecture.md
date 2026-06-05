# Архитектура интеграции Solr с Lucene

## Оглавление

1. [Обзор: слои интеграции](#1-обзор-слои-интеграции)
2. [DirectoryFactory: абстракция хранилища](#2-directoryfactory-абстракция-хранилища)
3. [SolrIndexConfig: конфигурация IndexWriter](#3-solrindexconfig-конфигурация-indexwriter)
4. [DefaultSolrCoreState: жизненный цикл IndexWriter](#4-defaultsolrcorestate-жизненный-цикл-indexwriter)
5. [SolrIndexWriter: обёртка над Lucene IndexWriter](#5-solrindexwriter-обёртка-над-lucene-indexwriter)
6. [IndexDeletionPolicyWrapper: управление commit-точками](#6-indexdeletionpolicywrapper-управление-commit-точками)
7. [SolrDeletionPolicy: политика очистки индекса](#7-solrdeletionpolicy-политика-очистки-индекса)
8. [MergePolicy: стратегии слияния сегментов](#8-mergepolicy-стратегии-слияния-сегментов)
9. [MergeScheduler: планировщик слияний](#9-mergescheduler-планировщик-слияний)
10. [RefCounted: потокобезопасный доступ к ресурсам](#10-refcounted-потокобезопасный-доступ-к-ресурсам)
11. [SolrIndexSearcher: обёртка над Lucene IndexReader](#11-solrindexsearcher-обёртка-над-lucene-indexreader)
12. [Управление searcher'ами в SolrCore](#12-управление-searcherами-в-solrcore)
13. [Warming: прогрев нового searcher'а](#13-warming-прогрев-нового-searcherа)
14. [NRT-поиск: Near Real Time](#14-nrt-поиск-near-real-time)
15. [Полный путь от старта SolrCore до рабочего IndexWriter](#15-полный-путь-от-старта-solrcore-до-рабочего-indexwriter)
16. [Полный путь открытия нового searcher'а](#16-полный-путь-открытия-нового-searcherа)
17. [Последовательность commit в Lucene](#17-последовательность-commit-в-lucene)
18. [Core reload: повторное использование IndexWriter](#18-core-reload-повторное-использование-indexwriter)
19. [Метрики IndexWriter](#19-метрики-indexwriter)
20. [Ключевые классы и файлы](#20-ключевые-классы-и-файлы)
21. [Справочная таблица параметров solrconfig.xml](#21-справочная-таблица-параметров-solrconfigxml)

---

## 1. Обзор: слои интеграции

```
solrconfig.xml <indexConfig>
        │
        ▼
SolrIndexConfig          ← парсинг конфигурации
        │
        ▼
IndexWriterConfig        ← Lucene: конфиг для IndexWriter
        │ (MergePolicy, MergeScheduler, ramBufferSizeMB, ...)
        ▼
SolrIndexWriter          ← Solr-обёртка над Lucene IndexWriter
  extends IndexWriter    ← перехват merge(), doAfterFlush()
        │
        ├── Directory    ← через DirectoryFactory
        │     ├── NRTCachingDirectory (default)
        │     ├── MMapDirectory
        │     ├── FSDirectory
        │     └── ByteBuffersDirectory (RAM)
        │
        ├── IndexDeletionPolicyWrapper
        │     └── SolrDeletionPolicy
        │
        ├── Codec        ← SchemaCodecFactory
        │
        └── IndexWriterConfig
              ├── MergePolicy    (TieredMergePolicy по умолчанию)
              ├── MergeScheduler (ConcurrentMergeScheduler по умолчанию)
              ├── Analyzer       (DelayedSchemaAnalyzer)
              └── IndexReaderWarmer (опционально)

DefaultSolrCoreState     ← хранит SolrIndexWriter, управляет lifecycle
        │
        └── RefCounted<IndexWriter>  ← потокобезопасный доступ

SolrIndexSearcher        ← Solr-обёртка над Lucene IndexReader
  wraps DirectoryReader  ← + UninvertingReader + ExitableDirectoryReader
        ├── filterCache
        ├── queryResultCache
        └── fieldValueCache
```

---

## 2. DirectoryFactory: абстракция хранилища

**Файл:** `solr/core/src/java/org/apache/solr/core/DirectoryFactory.java`

`DirectoryFactory` абстрагирует физическое хранилище индекса. Разные реализации дают разные trade-off между скоростью, памятью и надёжностью.

### Иерархия реализаций

```
DirectoryFactory (abstract)
└── CachingDirectoryFactory (abstract)
      ├── StandardDirectoryFactory   ← FSDirectory с выбором lock factory
      │     └── NRTCachingDirectoryFactory  ← default: FSDirectory + RAM-кэш
      │         └── NIOFSDirectoryFactory
      ├── MMapDirectoryFactory       ← memory-mapped файлы
      └── RAMDirectoryFactory        ← ByteBuffersDirectory (тесты/temp)
          └── EphemeralDirectoryFactory
```

### CachingDirectoryFactory: reference counting

`CachingDirectoryFactory` кэширует Directory-объекты по пути на диске и ведёт reference counting:

```java
class CacheValue {
    String path;
    Directory directory;
    int refCnt = 1;           // количество активных пользователей
    boolean doneWithDir;      // помечен для закрытия
    Set<CacheValue> removeEntries;
    Set<CacheValue> closeEntries;
}

// Получить Directory (создать или вернуть из кэша)
get(path, DirContext, lockType):
    CacheValue cv = byPathCache.get(path)
    if cv != null:
        cv.refCnt++
        return cv.directory
    // Создать новый через create() реализации
    directory = create(path, lockFactory, dirContext)
    byPathCache.put(path, new CacheValue(path, directory))
    return directory

// Освободить ссылку
release(directory):
    cv = byDirectoryCache.get(directory)
    cv.refCnt--
    if cv.refCnt == 0 && cv.doneWithDir:
        closeCacheValue(cv)  // физически закрыть

// Пометить как "более не нужен"
doneWithDirectory(directory):
    cv.doneWithDir = true
    if cv.refCnt == 0:
        closeCacheValue(cv)
```

Этот паттерн позволяет безопасно разделять один Directory между несколькими IndexWriter/IndexReader.

### NRTCachingDirectoryFactory (по умолчанию)

```java
// NRTCachingDirectoryFactory.java
protected Directory create(String path, LockFactory lockFactory, DirContext ctx) {
    return new NRTCachingDirectory(
        FSDirectory.open(Path.of(path), lockFactory),
        maxMergeSizeMB,   // default = 4 MB
        maxCachedMB);     // default = 48 MB
}
```

`NRTCachingDirectory` (Lucene) — это обёртка над FSDirectory, которая кэширует в RAM файлы новых сегментов при слиянии:

- **Что кэшируется:** файлы сегментов, создаваемых при merge, размером ≤ `maxMergeSizeMB`
- **Почему:** свежесозданные сегменты сразу читаются для построения NRT-reader'а; держать их в RAM ускоряет первые чтения
- **Вытеснение:** когда суммарный размер кэша превышает `maxCachedMB`, файлы вытесняются на диск
- **Прозрачность:** IndexWriter не знает о кэшировании; работает как обычный Directory

### MMapDirectoryFactory

```xml
<directoryFactory name="DirectoryFactory"
    class="solr.MMapDirectoryFactory">
  <bool name="preload">false</bool>
</directoryFactory>
```

`MMapDirectory` отображает файлы индекса в виртуальную память через `mmap(2)`. ОС управляет кэшированием страниц. Выгоды:
- Не занимает Java heap
- Работает с page cache ОС
- На современных 64-битных системах с достаточным RAM — быстрее FSDirectory

Параметр `preload=true` вызывает `madvise(MADV_WILLNEED)` для всех сегментов при открытии — полезно для SSD, избегает page faults при первых поисках.

### StandardDirectoryFactory

```xml
<directoryFactory name="DirectoryFactory"
    class="solr.StandardDirectoryFactory">
  <str name="lockType">native</str>
</directoryFactory>
```

Использует `FSDirectory.open()` с выбором lock factory:

| lockType | Реализация | Когда |
|---|---|---|
| `native` | `NativeFSLockFactory` | По умолчанию; OS-level flock/fcntl |
| `simple` | `SimpleFSLockFactory` | Создаёт файл-маркер; медленнее |
| `single` | `SingleInstanceLockFactory` | Один процесс; не для shared storage |
| `none` | `NoLockFactory` | Тесты; никогда в продакшене |

### RAMDirectoryFactory

```java
// ByteBuffersDirectory — хранит данные в байтовых буферах на heap
// Используется для тестов и временных индексов
// isPersistent() = false — индекс теряется при закрытии
```

---

## 3. SolrIndexConfig: конфигурация IndexWriter

**Файл:** `solr/core/src/java/org/apache/solr/update/SolrIndexConfig.java`

Парсит секцию `<indexConfig>` в `solrconfig.xml` и создаёт `IndexWriterConfig`.

### Параметры и дефолты

```java
// SolrIndexConfig defaults (из private constructor):
useCompoundFile        = false      // отдельные файлы для сегмента
maxBufferedDocs        = -1         // отключено (используется ramBuffer)
ramBufferSizeMB        = 100.0      // 100 МБ буфер перед flush
ramPerThreadHardLimitMB= -1         // отключено
maxCommitMergeWaitMillis= -1        // отключено
lockType               = "native"   // NativeFSLockFactory
```

### toIndexWriterConfig()

```java
public IndexWriterConfig toIndexWriterConfig(SolrCore core) {
    IndexWriterConfig iwc = new IndexWriterConfig(
        new DelayedSchemaAnalyzer(core));   // ← не инициализировать схему раньше времени

    if (maxBufferedDocs != -1)
        iwc.setMaxBufferedDocs(maxBufferedDocs);
    iwc.setRAMBufferSizeMB(ramBufferSizeMB);
    if (ramPerThreadHardLimitMB != -1)
        iwc.setRAMPerThreadHardLimitMB(ramPerThreadHardLimitMB);
    if (maxCommitMergeWaitMillis > 0)
        iwc.setMaxFullFlushMergeWaitMillis(maxCommitMergeWaitMillis);

    iwc.setUseCompoundFile(useCompoundFile);

    // MergePolicy (через factory)
    iwc.setMergePolicy(buildMergePolicy(schema, loader));

    // MergeScheduler
    iwc.setMergeScheduler(buildMergeScheduler(loader));

    // MergedSegmentWarmer (опционально)
    if (mergedSegmentWarmerInfo != null)
        iwc.setMergedSegmentWarmer(buildMergedSegmentWarmer(loader));

    // InfoStream для отладки
    iwc.setInfoStream(infoStream);

    // Сортировка индекса (если задана через SortingMergePolicy)
    Sort indexSort = buildIndexSort(schema, loader);
    if (indexSort != null) iwc.setIndexSort(indexSort);

    return iwc;
}
```

### buildMergePolicy()

```java
private MergePolicy buildMergePolicy(IndexSchema schema, SolrResourceLoader loader) {
    String mpfClassName = mergePolicyFactoryInfo != null
        ? mergePolicyFactoryInfo.className
        : DefaultMergePolicyFactory.class.getName();

    MergePolicyFactory mpf = loader.newInstance(mpfClassName, MergePolicyFactory.class);
    MergePolicyFactoryArgs args = new MergePolicyFactoryArgs(mergePolicyFactoryInfo);
    return mpf.getMergePolicy(args, schema);
}
```

### DelayedSchemaAnalyzer

```java
// Wrapper-analyzer который делегирует в IndexSchema.getIndexAnalyzer()
// Используется чтобы не инициализировать схему до полной загрузки SolrCore
// при создании IndexWriterConfig
class DelayedSchemaAnalyzer extends DelegatingAnalyzerWrapper {
    SolrCore core;
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        return core.getLatestSchema().getIndexAnalyzer();
    }
}
```

---

## 4. DefaultSolrCoreState: жизненный цикл IndexWriter

**Файл:** `solr/core/src/java/org/apache/solr/update/DefaultSolrCoreState.java`

`DefaultSolrCoreState` — синглтон-состояние, связывающий `SolrCore` с `IndexWriter`. Пережживает reload core'а.

### Ключевые поля

```java
private SolrIndexWriter indexWriter = null;        // текущий писатель
private RefCounted<IndexWriter> refCntWriter;      // ref-counted обёртка

private final ReentrantReadWriteLock iwLock;       // Read = использование IW
                                                   // Write = смена IW
private final ReentrantLock commitLock;            // только один hard commit одновременно
private final ReentrantLock recoveryLock;          // только один recovery одновременно

private volatile RecoveryStrategy recoveryStrat;   // текущая recovery стратегия
```

### getIndexWriter() — ленивое создание и ref-counting

```java
public RefCounted<IndexWriter> getIndexWriter(SolrCore core) throws IOException {
    lock(iwLock.readLock());  // захватить READ lock (держится пока используется IW)
    try {
        synchronized (this) {
            if (indexWriter == null) {
                indexWriter = createMainIndexWriter(core, "DirectUpdateHandler2");
            }
            initRefCntWriter();
            refCntWriter.incref();   // +1 для caller'а
            return refCntWriter;     // caller должен вызвать decref() когда готов
        }
    } catch (...) {
        iwLock.readLock().unlock();  // в случае ошибки
    }
}
```

**Ключевое:** `decref()` на возвращённом `RefCounted` **освобождает read lock**:

```java
refCntWriter = new RefCounted<IndexWriter>(indexWriter) {
    @Override
    public void decref() {
        iwLock.readLock().unlock();  // ← освобождаем read lock
        super.decref();
    }
    @Override
    public void close() {
        // Закрытие IndexWriter происходит отдельно (не при decref)
    }
};
```

Это означает: пока кто-то держит `RefCounted<IndexWriter>`, смена IndexWriter (write lock) заблокирована.

### changeWriter() — замена IndexWriter

```java
// changeWriter() захватывает WRITE lock:
private void changeWriter(SolrCore core, boolean rollback, boolean openNewWriter) {
    // Все держатели read lock должны завершить работу сначала
    lock(iwLock.writeLock());
    try {
        refCntWriter = null;          // обнулить wrapper
        IndexWriter old = indexWriter;
        indexWriter = null;

        if (rollback) old.rollback();
        else old.close();

        if (openNewWriter) {
            indexWriter = createMainIndexWriter(core, "DirectUpdateHandler2");
            initRefCntWriter();
        }
    } finally {
        iwLock.writeLock().unlock();
    }
}
```

### Счётчик ссылок на SolrCoreState

```java
// В SolrCoreState:
private int solrCoreStateRefCnt = 1;  // начальное значение при создании

increfSolrCoreState():
    solrCoreStateRefCnt++

decrefSolrCoreState(IndexWriterCloser closer):
    if (--solrCoreStateRefCnt == 0):
        close(closer)   // закрыть IndexWriter и DirectoryFactory
```

При **reload core**: новый SolrCore создаётся с тем же SolrCoreState (`incref`). Когда старый SolrCore закрывается (`decref`), IndexWriter не закрывается — ещё одна ссылка активна. Когда закрывается и новый — только тогда происходит реальное закрытие.

---

## 5. SolrIndexWriter: обёртка над Lucene IndexWriter

**Файл:** `solr/core/src/java/org/apache/solr/update/SolrIndexWriter.java`

`SolrIndexWriter extends IndexWriter` — тонкая обёртка, которая добавляет:
1. Интеграцию с `DirectoryFactory` (ref-counted release при закрытии)
2. Метрики слияний (major/minor merges)
3. Метаданные в commit (время, версия команды)
4. Корректное закрытие с защитой от двойного close

### Фабричный метод create()

```java
public static SolrIndexWriter create(
        SolrCore core, String name, String path,
        DirectoryFactory directoryFactory,
        boolean create, IndexSchema schema,
        SolrIndexConfig config, IndexDeletionPolicy delPolicy, Codec codec) {

    // 1. Получить Directory из фабрики (инкремент ref count)
    Directory d = directoryFactory.get(path, DirContext.DEFAULT, config.lockType);
    try {
        // 2. Создать SolrIndexWriter
        SolrIndexWriter w = new SolrIndexWriter(
            core, name, path, d, create, schema, config, delPolicy, codec);
        w.setDirectoryFactory(directoryFactory);
        return w;
    } finally {
        // 3. При неудаче — немедленно освободить Directory
        if (w == null && d != null) {
            directoryFactory.doneWithDirectory(d);
            directoryFactory.release(d);
        }
    }
}
```

### Внутренний конструктор

```java
private SolrIndexWriter(SolrCore core, String name, String path, Directory directory,
                         boolean create, IndexSchema schema, SolrIndexConfig config,
                         IndexDeletionPolicy delPolicy, Codec codec) {
    super(directory,
          config.toIndexWriterConfig(core)
                .setOpenMode(create ? CREATE : APPEND)
                .setIndexDeletionPolicy(delPolicy)
                .setCodec(codec));
    // ...
    numOpens.incrementAndGet();    // глобальный счётчик открытий (для мониторинга)
}
```

### setCommitData() — метаданные в commit

```java
public static void setCommitData(IndexWriter iw, long commitCommandVersion,
                                  Map<String, String> commitData) {
    Map<String, String> finalData = new HashMap<>(commitData);
    finalData.put(COMMIT_TIME_MSEC_KEY, String.valueOf(System.currentTimeMillis()));
    finalData.put(COMMIT_COMMAND_VERSION, String.valueOf(commitCommandVersion));
    iw.setLiveCommitData(finalData.entrySet());
}
```

Метаданные commit хранятся в `segments_N` файле. Используются при репликации (версия) и диагностике.

### merge() — перехват для метрик

```java
@Override
protected void merge(MergePolicy.OneMerge merge) throws IOException {
    boolean major = merge.totalNumDocs() > majorMergeDocs;   // default: 512K docs
    
    if (major) {
        runningMajorMerges.incrementAndGet();
        runningMajorMergesDocs.addAndGet(totalNumDocs);
        context = majorMerge.time();
    } else {
        runningMinorMerges.incrementAndGet();
        context = minorMerge.time();
    }
    
    try {
        super.merge(merge);    // реальное слияние Lucene
    } catch (Throwable t) {
        mergeErrors.inc();
        throw t;
    } finally {
        context.stop();
        // декремент счётчиков
    }
}
```

**major merge** = > 512K документов (настраивается через `<metrics><long name="majorMergeDocs">`)

### cleanup() — идемпотентное закрытие

```java
private void cleanup() {
    boolean doClose = false;
    synchronized (CLOSE_LOCK) {
        if (!isClosed) {
            doClose = true;
            isClosed = true;      // ← защита от двойного close
        }
    }
    if (doClose) {
        IOUtils.closeQuietly(infoStream);
        numCloses.incrementAndGet();
        directoryFactory.release(directory);   // ← декремент ref count Directory
        solrMetricsContext.unregister();
    }
}
```

---

## 6. IndexDeletionPolicyWrapper: управление commit-точками

**Файл:** `solr/core/src/java/org/apache/solr/core/IndexDeletionPolicyWrapper.java`

Lucene позволяет иметь несколько commit-точек (каждый `IndexWriter.commit()` создаёт новую). `IndexDeletionPolicyWrapper` управляет тем, какие из них можно удалять.

### Почему нужно удерживать старые commit-точки?

1. **Репликация:** реплика скачивает индекс с лидера. Пока скачивает — лидер не должен удалять файлы этой версии
2. **Снапшоты:** `/admin/cores?action=BACKUPINDEX` создаёт именованный снапшот, который нельзя удалять
3. **Откат:** в некоторых сценариях нужно вернуться к более старой версии

### Структуры данных

```java
volatile Map<Long, IndexCommit> knownCommits;     // generation → IndexCommit
volatile IndexCommit latestCommit;                // последний commit (всегда защищён)
Map<Long, Long> reserves;                          // generation → nanoTime истечения
Map<Long, AtomicInteger> savedCommits;             // generation → ref count (постоянные)
```

### Механизм резервирования (temporal reserve)

```java
// Зарезервировать commit на время репликации:
void setReserveDuration(Long generation, long reserveTimeMs) {
    long expiresAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(reserveTimeMs, MILLISECONDS);
    reserves.merge(generation, expiresAt, BinaryOperator.maxBy(...));
}

// Очистить истёкшие резервы:
void cleanReserves() {
    long now = System.nanoTime();
    reserves.entrySet().removeIf(e -> e.getValue() < now);
}
```

`ReplicationHandler` при старте репликации вызывает `setReserveDuration(generation, 1800000)` (30 минут). Это гарантирует что файлы этой версии индекса не будут удалены во время копирования.

### Сохранённые commit-точки (reference-counted saves)

```java
// Для снапшотов — постоянное сохранение с ref count:
void saveCommitPoint(Long generation) {
    savedCommits.computeIfAbsent(generation, k -> new AtomicInteger()).incrementAndGet();
}

void releaseCommitPoint(Long generation) {
    if (savedCommits.get(generation).decrementAndGet() <= 0) {
        savedCommits.remove(generation);
    }
}
```

### IndexCommitWrapper: перехват delete()

```java
// Каждый IndexCommit оборачивается в IndexCommitWrapper:
class IndexCommitWrapper extends IndexCommit {
    @Override
    public void delete() {
        synchronized (IndexDeletionPolicyWrapper.this) {
            if (isProtected(gen)) return;  // НЕ удалять!
            delegate.delete();              // удалить через оригинальную политику
        }
    }

    private boolean isProtected(long gen) {
        return System.nanoTime() < reserves.getOrDefault(gen, 0L)  // временный резерв
            || savedCommits.containsKey(gen)                        // снапшот
            || snapshotMgr.isSnapshotted(gen)                       // Core snapshot API
            || (latestCommit != null && gen == latestCommit.getGeneration()); // последний
    }
}
```

---

## 7. SolrDeletionPolicy: политика очистки индекса

**Файл:** `solr/core/src/java/org/apache/solr/core/SolrDeletionPolicy.java`

`SolrDeletionPolicy` реализует `IndexDeletionPolicy` Lucene. Вызывается `IndexWriter` после каждого commit для решения, какие старые commit-точки удалить.

### Параметры

```xml
<indexConfig>
  <deletionPolicy class="solr.SolrDeletionPolicy">
    <str name="maxCommitsToKeep">1</str>
    <str name="maxOptimizedCommitsToKeep">0</str>
    <str name="maxCommitAge">1DAY</str>
  </deletionPolicy>
</indexConfig>
```

| Параметр | Default | Описание |
|---|---|---|
| `maxCommitsToKeep` | 1 | Хранить последние N commit-точек |
| `maxOptimizedCommitsToKeep` | 0 | Дополнительно хранить N оптимизированных commit'ов |
| `maxCommitAge` | null | Удалять commit'ы старше этого возраста |

### onInit() и onCommit()

```java
// Вызывается при открытии IndexWriter (список всех существующих commit'ов)
public synchronized void onInit(List<? extends IndexCommit> commits) {
    applyDeletePolicy(commits);
}

// Вызывается после каждого commit()
public synchronized void onCommit(List<? extends IndexCommit> commits) {
    applyDeletePolicy(commits);
}

private void applyDeletePolicy(List<? extends IndexCommit> commits) {
    // Всегда защищаем последний commit
    // Применяем maxCommitsToKeep, maxOptimizedCommitsToKeep, maxCommitAge
    // Остальные помечаем через commit.delete()
    // IndexDeletionPolicyWrapper может заблокировать фактическое удаление
}
```

---

## 8. MergePolicy: стратегии слияния сегментов

### Зачем нужны merge?

Lucene пишет каждый flush как отдельный **сегмент** (набор файлов: `.si`, `.doc`, `.pos`, `.fdt`, `.fdx`, `.nvd`, `.nvm`, `_X.cfs`). Со временем накапливаются десятки и сотни мелких сегментов, что замедляет поиск (надо читать каждый). Merge объединяет несколько сегментов в один, убирая удалённые документы.

### Иерархия MergePolicyFactory

```
MergePolicyFactory (abstract)
└── SimpleMergePolicyFactory (abstract)
      ├── TieredMergePolicyFactory    ← default
      ├── LogDocMergePolicyFactory    ← legacy
      ├── LogByteSizeMergePolicyFactory
      ├── NoMergePolicyFactory        ← отключить merge
      └── SortingMergePolicyFactory   ← wrapper для сортированного индекса
          └── UpgradeIndexMergePolicyFactory
```

### DefaultMergePolicyFactory

Возвращает `TieredMergePolicy` с дефолтными параметрами Lucene. Используется если `<mergePolicyFactory>` не задан в solrconfig.xml.

### TieredMergePolicy (рекомендуется)

```xml
<mergePolicyFactory class="org.apache.solr.index.TieredMergePolicyFactory">
  <int name="maxMergeAtOnce">10</int>
  <double name="segmentsPerTier">10.0</double>
  <double name="maxMergedSegmentMB">5120.0</double>
  <double name="floorSegmentMB">2.0</double>
  <double name="forceMergeDeletesPctAllowed">10.0</double>
  <double name="deletesPctAllowed">20.0</double>
</mergePolicyFactory>
```

**Алгоритм TieredMergePolicy:**

```
Сегменты сортируются по размеру (убывание).
Определяются "tiers" (уровни) по логарифмической шкале:
  tier 0: самые большие сегменты (≈ maxMergedSegmentMB)
  tier 1: средние сегменты
  tier N: маленькие сегменты (≤ floorSegmentMB → floorSegmentMB)

Для каждого tier:
  if количество сегментов > segmentsPerTier:
    Выбрать maxMergeAtOnce сегментов для слияния
    Приоритет: сегменты с большим % удалённых документов

Ограничения:
  Итоговый сегмент не должен быть > maxMergedSegmentMB
  forceMergeDeletesPctAllowed: при expungeDeletes — порог удалений
```

| Параметр | Default | Описание |
|---|---|---|
| `maxMergeAtOnce` | 10 | Максимум сегментов в одном merge |
| `segmentsPerTier` | 10 | Целевое количество сегментов на уровень |
| `maxMergedSegmentMB` | 5120 (5 ГБ) | Максимальный размер результирующего сегмента |
| `floorSegmentMB` | 2 | Минимальный размер для tier-расчёта (маленькие сегменты = 2 МБ) |
| `forceMergeDeletesPctAllowed` | 10% | Порог % удалений при expungeDeletes |
| `deletesPctAllowed` | 20% | Порог % удалений для выбора кандидатов на merge |

### LogDocMergePolicy (устаревшая)

```xml
<mergePolicyFactory class="org.apache.solr.index.LogDocMergePolicyFactory">
  <int name="mergeFactor">10</int>
  <int name="maxMergeDocs">2147483647</int>
</mergePolicyFactory>
```

**Алгоритм:** каждый раз когда число сегментов достигает `mergeFactor`, они объединяются в один. Более предсказуем, но менее эффективен для переменных размеров.

### SortingMergePolicyFactory (индекс со сортировкой)

```xml
<mergePolicyFactory class="org.apache.solr.index.SortingMergePolicyFactory">
  <str name="sort">timestamp desc</str>
  <str name="wrapped.prefix">inner</str>
  <lst name="inner">
    <str name="class">org.apache.solr.index.TieredMergePolicyFactory</str>
  </lst>
</mergePolicyFactory>
```

Оборачивает другую политику и при каждом merge сортирует документы по указанному полю. Используется с `<indexConfig><indexSort>` для ускорения range-запросов по отсортированному полю.

### useCompoundFile: влияние на merge

```xml
<useCompoundFile>false</useCompoundFile>  <!-- рекомендуется -->
```

- `true`: после merge все файлы сегмента упаковываются в `.cfs` (compound file format). Меньше файловых дескрипторов, удобнее для репликации (копировать один файл). Медленнее при открытии (декомпрессия).
- `false` (default): каждый компонент сегмента хранится отдельно. Быстрее merge и открытие.

---

## 9. MergeScheduler: планировщик слияний

### ConcurrentMergeScheduler (по умолчанию)

```xml
<mergeScheduler class="org.apache.lucene.index.ConcurrentMergeScheduler">
  <int name="maxThreadCount">1</int>
  <int name="maxMergeCount">4</int>
  <bool name="ioThrottle">true</bool>
</mergeScheduler>
```

Выполняет merge в фоновых потоках:

| Параметр | Default (auto) | Описание |
|---|---|---|
| `maxThreadCount` | `min(4, cpus/2)` | Максимум параллельных merge |
| `maxMergeCount` | `maxThreadCount+5` | Максимум ожидающих merge в очереди |
| `ioThrottle` | true | Авто-throttling IO при накоплении merge |

**IO throttling:** `ConcurrentMergeScheduler` адаптивно замедляет merge если операции записи слишком быстрые (чтобы не вытеснять reading IO). Отключается через `disableAutoIOThrottle()` / параметр `ioThrottle=false`.

**Back-pressure:** если количество ожидающих merge превышает `maxMergeCount`, IndexWriter блокирует новые flush до завершения некоторых merge. Это предотвращает бесконечный рост сегментов при высокой нагрузке.

### SerialMergeScheduler

Выполняет merge в том же потоке что и IndexWriter. Используется для тестов или специальных случаев когда параллелизм нежелателен.

---

## 10. RefCounted: потокобезопасный доступ к ресурсам

**Файл:** `solr/core/src/java/org/apache/solr/util/RefCounted.java`

```java
public abstract class RefCounted<Type> {
    protected final Type resource;
    protected final AtomicInteger refcount = new AtomicInteger();

    public final RefCounted<Type> incref() {
        refcount.incrementAndGet();
        return this;
    }

    public void decref() {
        if (refcount.decrementAndGet() == 0) {
            close();    // абстрактный — реализует cleanup
        }
    }

    public Type get() { return resource; }

    protected abstract void close();
}
```

**Использование для IndexWriter:**
```java
RefCounted<IndexWriter> iw = core.getUpdateHandler().getUpdateLog().getIndexWriterFromCore();
try {
    IndexWriter writer = iw.get();
    // работа с writer...
} finally {
    iw.decref();    // обязательно! иначе read lock никогда не снимется
}
```

**Использование для IndexSearcher:**
```java
RefCounted<SolrIndexSearcher> holder = core.getSearcher();
try {
    SolrIndexSearcher searcher = holder.get();
    // поиск...
} finally {
    holder.decref();    // обязательно! иначе searcher никогда не закроется
}
```

**Защита от race при close:**
```java
// В SolrCore.newHolder():
public void close() {
    synchronized (searcherLock) {
        if (refcount.get() > 0) return;   // кто-то успел инкрементировать — не закрывать
        searcherList.remove(this);
    }
    resource.close();
}
```

---

## 11. SolrIndexSearcher: обёртка над Lucene IndexReader

**Файл:** `solr/core/src/java/org/apache/solr/search/SolrIndexSearcher.java`

### Слои оборачивания IndexReader

```
DirectoryReader (Lucene, базовый)
    │
    ▼
UninvertingReader.wrap(reader, ...)
    │   Добавляет доступ к полям через DocValues из stored fields
    │   (для полей без нативных DocValues — обратная инверсия при первом обращении)
    ▼
ExitableDirectoryReader.wrap(reader, ...)  (опционально)
    │   Добавляет проверку таймаута и interrupt при итерации
    ▼
SolrIndexSearcher extends IndexSearcher
    │   + schema-awareness
    │   + три кэша
    │   + SolrDocumentFetcher
    └── rawReader: оригинальный DirectoryReader (для NRT reopen)
```

### Конструктор и инициализация кэшей

```java
public SolrIndexSearcher(SolrCore core, String path, IndexSchema schema,
                          SolrIndexConfig config, String name, boolean enableCaches,
                          DirectoryFactory directoryFactory) {
    // 1. Открыть DirectoryReader из директории
    // 2. Обернуть в UninvertingReader
    // 3. Обернуть в ExitableDirectoryReader
    // 4. Передать в super (IndexSearcher)

    if (enableCaches) {
        // filterCache: DocSet-ы для filter-запросов
        filterCache = config.filterCache.newInstance();

        // queryResultCache: топ-N результатов для полных запросов
        queryResultCache = config.queryResultCache.newInstance();

        // fieldValueCache: UnInvertedField-ы (инверсия для sort/facet)
        fieldValueCache = config.fieldValueCache.newInstance();

        cacheList = new SolrCache[] {filterCache, queryResultCache, fieldValueCache};
    }

    // SolrDocumentFetcher: lazy загрузка stored fields
    docFetcher = new SolrDocumentFetcher(this, config, enableCaches);
}
```

### getIndexFingerprint() — кэш fingerprint для PeerSync

```java
public IndexFingerprint getIndexFingerprint(long maxVersion) throws IOException {
    IndexReader.CacheHelper cacheHelper = getIndexReader().getReaderCacheHelper();
    // Кэш привязан к lifecycle reader'а (очищается при его закрытии)
    // Ключ: maxVersion (обычно Long.MAX_VALUE для полного fingerprint)
    return perSegmentFingerprintCache.computeIfAbsent(
        new Pair<>(cacheHelper.getKey(), maxVersion),
        k -> IndexFingerprint.getFingerprint(this, maxVersion));
}
```

---

## 12. Управление searcher'ами в SolrCore

### Четыре типа searcher'ов

```java
// SolrCore.java
private RefCounted<SolrIndexSearcher> _searcher;            // активный (registered)
private ArrayDeque<RefCounted<SolrIndexSearcher>> _searchers; // все "нормальные" searchers
private RefCounted<SolrIndexSearcher> realtimeSearcher;      // NRT searcher
private ArrayDeque<RefCounted<SolrIndexSearcher>> _realtimeSearchers; // все RT searchers
```

### onDeckSearchers — счётчик "в процессе открытия"

```java
private int onDeckSearchers;       // сколько searchers сейчас открываются/прогреваются
private int maxWarmingSearchers;   // лимит параллельного warming (из конфига)
```

### getSearcher() — полный алгоритм

```
getSearcher(forceNew, returnSearcher, waitSearcher, updateHandlerReopens):

1. synchronized(searcherLock):
   if (!forceNew && _searcher != null):
     _searcher.incref()
     return _searcher  // быстрый путь

2. Проверить лимит onDeckSearchers:
   if onDeckSearchers >= maxWarmingSearchers:
     searcherLock.wait()  // ждать пока один из warming завершится
   onDeckSearchers++

3. openSearcherLock.lock()  // сериализовать открытие нескольких searchers
   try:
     newSearcher = openNewSearcher(updateHandlerReopens, realtime=false)
   finally:
     openSearcherLock.unlock()

4. searcherExecutor.submit(():  // АСИНХРОННО (один поток на все warming)
   try:
     if (currSearcher != null):
       // Прогрев: скопировать "горячие" данные из старого кэша
       newSearcher.warm(currSearcher)
       // Уведомить listeners (например, SolrEventListener)
       newSearcherListeners.forEach(l -> l.newSearcher(newSearcher, currSearcher))
     else:
       // Первый searcher
       newSearcher.bootstrapFirstSearcher()
       firstSearcherListeners.forEach(l -> l.newSearcher(newSearcher, null))
   finally:
     registerSearcher(newSearcher)  // атомарно заменить _searcher
     onDeckSearchers--
     searcherLock.notifyAll()
   )

5. if returnSearcher:
   newSearcher.incref()   // ещё один +1 для caller'а
   return newSearcher
```

### registerSearcher() — атомарная замена

```java
private void registerSearcher(RefCounted<SolrIndexSearcher> newHolder) {
    synchronized (searcherLock) {
        // Заменить _searcher
        RefCounted<SolrIndexSearcher> old = _searcher;
        _searcher = newHolder;

        if (old != null) {
            old.decref();    // -1: _searcher больше не держит ссылку
        }

        // Зарегистрировать кэши в инфо-реестре
        newHolder.get().register();

        // Логировать время прогрева
        log.info("Registered new searcher, warmup: {}ms",
            newHolder.get().getWarmupTime());
    }
}
```

---

## 13. Warming: прогрев нового searcher'а

**Зачем:** новый IndexReader открывает новые segment-файлы. Кэши пусты. Если сразу отдать его клиентам, первые запросы будут медленными. Warming копирует "горячие" данные из старого кэша.

### warm() в SolrIndexSearcher

```java
public void warm(SolrIndexSearcher old) {
    long warmingStartTime = System.nanoTime();

    // Прогреть каждый кэш:
    for (SolrCache cache : cacheList) {
        cache.warm(this, old.getCacheByName(cache.name()));
        // filterCache.warm():
        //   для каждой записи в старом кэше, заново выполнить запрос в новом searcher'е
        //   (или сохранить если сегменты не изменились)
        // queryResultCache.warm():
        //   выполнить top-N запросы из старого кэша в новом searcher'е
        //   (только если configured autowarmCount > 0)
    }

    warmupTime = TimeUnit.MILLISECONDS.convert(
        System.nanoTime() - warmingStartTime, TimeUnit.NANOSECONDS);
}
```

### autowarmCount

```xml
<!-- В solrconfig.xml: -->
<filterCache size="512" initialSize="512" autowarmCount="256"/>
<queryResultCache size="512" initialSize="512" autowarmCount="128"/>
```

`autowarmCount` — сколько записей из старого кэша переносить в новый. `0` = не прогревать.

### newSearcherListeners и firstSearcherListeners

```xml
<listener event="newSearcher" class="solr.QuerySenderListener">
  <arr name="queries">
    <lst><str name="q">*:*</str><str name="rows">0</str></lst>
  </arr>
</listener>

<listener event="firstSearcher" class="solr.QuerySenderListener">
  <arr name="queries">
    <lst><str name="q">*:*</str><str name="facet">true</str>...</lst>
  </arr>
</listener>
```

`QuerySenderListener` выполняет указанные запросы в контексте нового searcher'а — это заполняет кэши "прогревными" данными. Выполняется через `searcherExecutor` (однопоточный).

---

## 14. NRT-поиск: Near Real Time

NRT-поиск позволяет видеть обновления до hard commit. Реализован через `DirectoryReader.openIfChanged(currentReader, writer, applyAllDeletes)`.

### openNewSearcher с realtime=true

```java
// updateHandlerReopens=true, realtime=true
RefCounted<IndexWriter> iw = core.getUpdateHandler().getSolrCoreState().getIndexWriter(null);
try {
    IndexWriter writer = iw.get();
    // Открыть reader поверх незакоммиченных данных IndexWriter'а
    DirectoryReader newReader = DirectoryReader.openIfChanged(
        currentReader, writer, true);  // applyAllDeletes=true
    if (newReader == null) return currentHolder;  // ничего не изменилось

    // Создать SolrIndexSearcher с useCaches=false (RT searcher без кэшей)
    SolrIndexSearcher newSearcher = new SolrIndexSearcher(
        core, path, schema, config, "realtime", false, directoryFactory);
} finally {
    iw.decref();
}
```

Хранится в `_realtimeSearchers` (отдельно от `_searchers`). Используется только для `RealTimeGetComponent` — не для обычного поиска.

### Почему RT searchers без кэшей?

RT searcher обновляется при каждом soft commit (каждые N секунд). Строить и выбрасывать кэши при каждом обновлении — дорого. Поэтому RT searcher `useCaches=false` — только raw чтение документов.

### openRealtimeSearcher() в UpdateLog

```java
// UpdateLog.java
public void openRealtimeSearcher() {
    core.openNewSearcher(true, true);   // updateHandlerReopens=true, realtime=true
    // Очищает in-memory карты map/prevMap/prevMap2
    // После этого RTG-запросы идут через searcher, не через tlog-карты
}
```

---

## 15. Полный путь от старта SolrCore до рабочего IndexWriter

```
CoreContainer.load()
    │
    ▼
SolrCore constructor:
    │
    ├── 1. Создать DefaultSolrCoreState(directoryFactory)
    │         DirectoryFactory инициализируется из solrconfig.xml или default:
    │         NRTCachingDirectoryFactory
    │
    ├── 2. initDirectoryFactory():
    │         PluginInfo info = solrConfig.getPluginInfo("directoryFactory")
    │         directoryFactory = loader.newInstance(info.className)
    │         directoryFactory.init(info.initArgs)
    │
    ├── 3. initIndex(prev != null, reload):
    │         Если индекс пуст → создать (create=true для IndexWriter)
    │         Если переиспользуем prev → create=false
    │
    ├── 4. initWriters():
    │         updateHandler = new DirectUpdateHandler2(core, prev)
    │           → DirectUpdateHandler2 создаёт UpdateLog
    │           → getIndexWriter() → DefaultSolrCoreState.getIndexWriter(core)
    │               → indexWriter == null → createMainIndexWriter()
    │                   → SolrIndexWriter.create(core, name, path, directoryFactory,
    │                                             create, schema, indexConfig,
    │                                             deletionPolicy, codec)
    │                       → directoryFactory.get(path, DEFAULT, lockType)
    │                           → NRTCachingDirectory(FSDirectory.open(path), 4, 48)
    │                       → new IndexWriterConfig = indexConfig.toIndexWriterConfig(core)
    │                           → TieredMergePolicy
    │                           → ConcurrentMergeScheduler
    │                           → ramBufferSizeMB=100
    │                       → new SolrIndexWriter(d, config)
    │                           → super(d, config)  ← Lucene IndexWriter!
    │
    └── 5. initSearcher(prev):
              getSearcher(false, false, null, false)
              → openNewSearcher(false, false)
              → DirectoryReader.open(directory) или openIfChanged(prevReader)
              → new SolrIndexSearcher(core, ..., enableCaches=true)
              → warming через searcherExecutor
              → registerSearcher(_searcher = newHolder)
```

---

## 16. Полный путь открытия нового searcher'а

```
Триггер: autoSoftCommit срабатывает (например, каждую секунду)
    │
    ▼
DirectUpdateHandler2.commit(softCommit=true):
    │
    ├── ulog.preSoftCommit()         ← ротировать in-memory map
    ├── core.getSearcher(false, false, null, true)  ← openNew=true
    │
    ▼
SolrCore.getSearcher():
    │
    ├── synchronized(searcherLock):
    │       onDeckSearchers++
    │
    ├── openSearcherLock.lock()
    │     openNewSearcher(updateHandlerReopens=false, realtime=false):
    │       RefCounted<IW> iw = getUpdateHandler().getSolrCoreState().getIndexWriter(null)
    │       // Попытка NRT reopen:
    │       DirectoryReader newReader = DirectoryReader.openIfChanged(currentRawReader)
    │       if newReader == null:
    │           // Ничего не изменилось → вернуть текущий searcher (без создания нового)
    │           return currentSearcherHolder
    │       // Создать новый SolrIndexSearcher поверх newReader:
    │       SolrIndexSearcher newSearcher = new SolrIndexSearcher(...)
    │       // Добавить в _searchers деку
    │       return newHolder(newSearcher, _searchers)  // refcount=1
    │
    ├── searcherExecutor.submit(warming task):
    │     try:
    │       newSearcher.warm(currSearcher):
    │           for cache in cacheList:
    │               cache.warm(newSearcher, oldCache)  // copy-on-read cache entries
    │       fireNewSearcherListeners(newSearcher, currSearcher)
    │     finally:
    │       registerSearcher(newHolder):
    │           old = _searcher
    │           _searcher = newHolder
    │           old.decref()      // -1 для _searcher ссылки (закроется если refcount=0)
    │           newHolder.get().register()
    │       onDeckSearchers--
    │       searcherLock.notifyAll()
    │
    ▼
ulog.postSoftCommit()  ← clearOldMaps()
```

---

## 17. Последовательность commit в Lucene

```
DirectUpdateHandler2.commit(CommitUpdateCommand):
    │
    ├── [hard commit]:
    │     commitLock.lock()
    │
    ├── ulog.preCommit():
    │     newMap()           ← ротировать map → prevMap
    │     prevTlog = tlog    ← переключить tlog
    │     tlog = null
    │     id++
    │
    ├── SolrIndexWriter.setCommitData(writer, version, data):
    │     writer.setLiveCommitData([
    │         "commitTimeMSec" → currentTimeMillis,
    │         "commitCommandVer" → version
    │     ])
    │
    ├── RefCounted<IW> iw = getIndexWriter(core)
    │   iw.get().commit():
    │     [Lucene internals]:
    │       Flush все in-memory буферы → создать новые сегменты
    │       Слить сегменты согласно MergePolicy (sync/async)
    │       Написать segments_N файл
    │       fsync: channel.force(true)
    │       Атомарно переключить segments_N symlink
    │   iw.decref()
    │
    ├── ulog.postCommit():
    │     prevTlog.writeCommit(cmd)   ← COMMIT-маркер в tlog
    │     addOldLog(prevTlog, true)
    │
    └── [soft commit]:
          ulog.preSoftCommit()
          core.getSearcher(true, ...)   ← открыть новый NRT reader
          ulog.postSoftCommit()
```

---

## 18. Core reload: повторное использование IndexWriter

При `CoreContainer.reload(coreName)`:

```
1. prev = существующий SolrCore
   prev.solrCoreState.increfSolrCoreState()  // +1 (теперь refCnt=2)

2. Создать новый SolrCore(... state=prev.solrCoreState ...):
   // IndexWriter НЕ создаётся заново — тот же объект
   // DirectoryFactory НЕ пересоздаётся
   // Загружается новый IndexSchema, SolrConfig

3. prev.close():
   prev.solrCoreState.decrefSolrCoreState()  // -1 (refCnt=1)
   // IndexWriter НЕ закрывается — ещё одна ссылка активна

4. Когда новый SolrCore закрывается:
   state.decrefSolrCoreState()  // refCnt=0 → close IndexWriter
```

Это позволяет reload без потери буферизованных обновлений и без переоткрытия файлов индекса.

---

## 19. Метрики IndexWriter

`SolrIndexWriter` регистрирует метрики в категории `INDEX.*`:

| Метрика | Тип | Описание |
|---|---|---|
| `INDEX.merge.major` | Timer | Время major merge (> 512K docs) |
| `INDEX.merge.minor` | Timer | Время minor merge |
| `INDEX.merge.errors` | Counter | Количество ошибок merge |
| `INDEX.merge.major.docs` | Meter | Документов/сек через major merge |
| `INDEX.merge.major.deletedDocs` | Meter | Удалённых документов в major merge |
| `INDEX.merge.major.running` | Gauge | Текущих major merges |
| `INDEX.merge.minor.running` | Gauge | Текущих minor merges |
| `INDEX.merge.major.running.docs` | Gauge | Документов в текущих major merges |
| `INDEX.merge.minor.running.segments` | Gauge | Сегментов в текущих minor merges |
| `INDEX.flush` | Meter | Частота flush (из heap-буфера на диск) |
| `numOpens` | AtomicLong static | Всего открытий IndexWriter (глобально) |
| `numCloses` | AtomicLong static | Всего закрытий IndexWriter (глобально) |

Настройка в `solrconfig.xml`:
```xml
<indexConfig>
  <metrics>
    <bool name="merge">true</bool>        <!-- включить coarse-grained merge метрики -->
    <bool name="mergeDetails">true</bool>  <!-- включить fine-grained (docs, deletedDocs) -->
    <long name="majorMergeDocs">524288</long>  <!-- порог major merge (512K) -->
  </metrics>
</indexConfig>
```

---

## 20. Ключевые классы и файлы

| Класс | Файл | Роль |
|---|---|---|
| `SolrIndexConfig` | `update/SolrIndexConfig.java` | Парсинг `<indexConfig>`, создание IndexWriterConfig |
| `SolrIndexWriter` | `update/SolrIndexWriter.java` | Lucene IndexWriter + метрики merge + DirectoryFactory release |
| `DefaultSolrCoreState` | `update/DefaultSolrCoreState.java` | Lifecycle IndexWriter, ref count, commit/recovery lock |
| `DirectoryFactory` | `core/DirectoryFactory.java` | Абстракция хранилища |
| `CachingDirectoryFactory` | `core/CachingDirectoryFactory.java` | Reference-counted Directory cache |
| `NRTCachingDirectoryFactory` | `core/NRTCachingDirectoryFactory.java` | Default: FSDirectory + 48MB RAM cache |
| `MMapDirectoryFactory` | `core/MMapDirectoryFactory.java` | memory-mapped files |
| `RAMDirectoryFactory` | `core/RAMDirectoryFactory.java` | ByteBuffersDirectory (тесты) |
| `IndexDeletionPolicyWrapper` | `core/IndexDeletionPolicyWrapper.java` | Защита commit-точек для репликации и снапшотов |
| `SolrDeletionPolicy` | `core/SolrDeletionPolicy.java` | Политика хранения N commit-точек |
| `MergePolicyFactory` | `index/MergePolicyFactory.java` | Абстракция фабрики MergePolicy |
| `TieredMergePolicyFactory` | `index/TieredMergePolicyFactory.java` | Default merge policy |
| `SortingMergePolicyFactory` | `index/SortingMergePolicyFactory.java` | Сортированный индекс |
| `RefCounted` | `util/RefCounted.java` | Потокобезопасный ref-counted wrapper |
| `SolrIndexSearcher` | `search/SolrIndexSearcher.java` | IndexReader + 3 кэша + warming |
| `SolrCore` | `core/SolrCore.java` | Управление _searcher, openNewSearcher, warming |

---

## 21. Справочная таблица параметров solrconfig.xml

```xml
<indexConfig>
  <!-- DirectoryFactory -->
  <directoryFactory name="DirectoryFactory" class="solr.NRTCachingDirectoryFactory">
    <double name="maxMergeSizeMB">4</double>   <!-- кэшировать merge-сегменты ≤ 4MB -->
    <double name="maxCachedMB">48</double>     <!-- макс. RAM-кэш -->
  </directoryFactory>

  <!-- IndexWriter параметры -->
  <useCompoundFile>false</useCompoundFile>      <!-- false = отдельные файлы (быстрее) -->
  <ramBufferSizeMB>100</ramBufferSizeMB>        <!-- flush если heap > 100MB -->
  <maxBufferedDocs>-1</maxBufferedDocs>         <!-- flush по кол-ву docs (обычно -1) -->
  <ramPerThreadHardLimitMB>-1</ramPerThreadHardLimitMB>  <!-- per-thread лимит -->
  <maxCommitMergeWaitTime>-1</maxCommitMergeWaitTime>    <!-- мс ожидания sync merge -->
  <lockType>native</lockType>                   <!-- native / simple / single / none -->

  <!-- MergePolicy -->
  <mergePolicyFactory class="org.apache.solr.index.TieredMergePolicyFactory">
    <int name="maxMergeAtOnce">10</int>
    <double name="segmentsPerTier">10.0</double>
    <double name="maxMergedSegmentMB">5120.0</double>
    <double name="floorSegmentMB">2.0</double>
    <double name="deletesPctAllowed">20.0</double>
  </mergePolicyFactory>

  <!-- MergeScheduler -->
  <mergeScheduler class="org.apache.lucene.index.ConcurrentMergeScheduler">
    <int name="maxThreadCount">4</int>
    <int name="maxMergeCount">9</int>
    <bool name="ioThrottle">true</bool>
  </mergeScheduler>

  <!-- Опциональный прогрев сегментов после merge -->
  <mergedSegmentWarmer class="org.apache.lucene.index.SimpleMergedSegmentWarmer"/>

  <!-- DeletionPolicy -->
  <deletionPolicy class="solr.SolrDeletionPolicy">
    <str name="maxCommitsToKeep">1</str>
    <str name="maxOptimizedCommitsToKeep">0</str>
  </deletionPolicy>

  <!-- Метрики merge -->
  <metrics>
    <bool name="merge">true</bool>
    <bool name="mergeDetails">false</bool>
    <long name="majorMergeDocs">524288</long>
  </metrics>

  <!-- InfoStream для отладки IndexWriter -->
  <infoStream>false</infoStream>
</indexConfig>
```
