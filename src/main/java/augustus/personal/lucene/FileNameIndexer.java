package augustus.personal.lucene;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by thooaug on 24/8/2023.
 *
 * BENCHMARK:
 *  - [2023-11-04]
 *    - Java 17 -- 33 seconds (baseline)
 *    - Java 21 -- 18 seconds
 */
@Slf4j
public class FileNameIndexer implements Closeable {

    public static void main(String[] args) throws Exception {
        Path path = Path.of("lucene");
        DeleteAll.doDeleteAll(path);
        log.info("START INDEXING NOW");
        long elapsed = System.currentTimeMillis();
        try (FileNameIndexer indexer = new FileNameIndexer(path)) {
            File dir = new File(args[0]);
            indexer.indexFiles(dir);
        }
        elapsed = System.currentTimeMillis() - elapsed;
        elapsed = elapsed / 1000;
        log.info("DONE INDEXING AFTER {} seconds", elapsed);
    }

    int limit = 2097152;
    final StandardAnalyzer analyzer;
    final FSDirectory indexDirectory;
    final boolean TRACE = false;
    final ExecutorService pool;
    final List<Future<?>> tasks;

    String[] indexAllowedExts = new String[]{
            ".java", ".xml", ".yml", ".properties"
    };

    String[] excludeDirContains = new String[] {
            "artifactory", ".idea", "apache-ignite-src", "dbeaver", ".git", "target"
    };

    String indexAllowedRegex = null;

    public FileNameIndexer(Path path) throws IOException {
        analyzer = new StandardAnalyzer();
        indexDirectory = FSDirectory.open(path);
        pool = Executors.newVirtualThreadPerTaskExecutor();
        tasks = new ArrayList<>(100);
    }

    @Override
    public void close() throws IOException {
        closePool();
        analyzer.close();
        indexDirectory.close();
    }

    private void awaitTasksCompletion() {
        int completedCount = 0;
        try {
            Future<?> task;
            //do {
                while (!tasks.isEmpty()) {
                    synchronized (tasks) {
                        task = tasks.remove(0);
                    }
                    if (task.isDone()) {
                        ++completedCount;
                        continue;
                    }
                    try {
                        task.get(100, TimeUnit.MILLISECONDS);
                        ++completedCount;
                    } catch (TimeoutException e) {
                        // reinsert to retry later
                        synchronized (tasks) {
                            tasks.add(task);
                        }
                    }
                }
            //    Thread.sleep(100);
            //    log.debug("... {} TASKS REMAINING", tasks.size());
            //} while (!tasks.isEmpty());
            Thread.sleep(100);
        } catch (Exception e) {
            log.error("", e);
        }
        log.debug("{} TASKS COMPLETED, {} REMAINING", completedCount, tasks.size());
    }

    private void closePool() {
        pool.shutdown();
        //try {
        //    if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
        //        pool.shutdownNow();
        //    }
        //} catch (InterruptedException e) {
        //    log.error(e.toString());
        //}
        log.debug("VIRTUAL THREAD POOL SHUTDOWN");
    }

    public void indexFiles(File dir) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        try (IndexWriter indexWriter = new IndexWriter(indexDirectory, config)) {
            asyncIndexFilesRecursively(indexWriter, dir);
            awaitTasksCompletion();
            indexWriter.commit();
        }
    }

    private void asyncIndexFilesRecursively(IndexWriter indexWriter, File file) {
        Future<Object> task = pool.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                FileNameIndexer.this.indexFilesRecursively(indexWriter, file);
                return 0;
            }
        });
        synchronized (tasks) {
            tasks.add(task);
        }
    }

    private void indexFilesRecursively(IndexWriter indexWriter, File file) throws IOException {
        FileForIndex source = FileForIndex.from(file);
        if (!indexAllowed(source)) return;
        if (file.isDirectory()) {
            if (TRACE) log.trace("[INDEX_DIR ] -- {}", file.getAbsolutePath());
            File[] files = file.listFiles();
            if (files != null) {
                for (File subFile : files) {
                    asyncIndexFilesRecursively(indexWriter, subFile);
                }
            }
        } else {
            indexFile(indexWriter, source);
        }
    }

    private void indexFile(IndexWriter indexWriter, FileForIndex source) throws IOException {
        /*Document document = new Document();
        document.add(new StringField("filename", file.getAbsolutePath(), Field.Store.YES));
        try {
            document.add(new TextField("content", FileUtils.limitByLength(file, limit), Field.Store.NO));
        } catch (UncheckedIOException e) {
            log.warn("{} -- {}", file.getAbsolutePath(), e.toString());
            return;
        }*/
        Document document = source.asDocument(limit);
        if (document != null) {
            if (TRACE) log.trace("[INDEX_FILE] -- {}", source);
            indexWriter.addDocument(document);
        }
    }

    private boolean indexAllowed(FileForIndex source) {
        if (source.isFile()) {
            if (indexAllowedExts != null) {
                String ext = source.ext();
                for (String allowedExt : indexAllowedExts) {
                    if (allowedExt.equals(ext)) return true;
                }
            }
            if (indexAllowedRegex != null) {
                return source.name().matches(indexAllowedRegex);
            }
        }
        else if (source.isDirectory()) {
            if (excludeDirContains != null) {
                for (String token : excludeDirContains) {
                    if (source.path().contains(token)) {
                        if (TRACE) log.trace("[SKIP] -- {}", source.path());
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }
}
