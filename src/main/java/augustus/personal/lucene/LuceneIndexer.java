package augustus.personal.lucene;

import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collector;

/**
 * Created by Augustus Thoo on 4/24/2021
 */
@Slf4j
public class LuceneIndexer {

    private static final boolean debugEventCommit = true;
    private static final boolean debugEventDeleteAll = true;
    private static final boolean debugEventIndex = true;
    private static final boolean debugEventIndex_Content = false;

    @Setter
    public static class Config {
        private String path = "lucene";
        // private int indexTaskQueueLimit = 65536;
        private int fileContentLimit = 2097152; // 2MB

        private String[] indexAllowedExts = new String[]{
                ".java", ".xml", ".yml", ".properties"
        };

        private String[] excludeDirContains = new String[] {
                "artifactory", ".idea", "apache-ignite-src"
        };

        private String indexAllowedRegex = null;
    }

    private Config config = new Config();

    private List<Closeable> closeables;
    private Analyzer analyzer;
    private Directory indexDirectory;
    private IndexWriter indexWriter;
    private Scheduler scheduler;

    public void setConfig(Config config) {
        this.config = config;
    }

    @PostConstruct
    public void init() throws Exception {
        closeables = new ArrayList<>();
        initAnalyzer();
        initDirectory();
        initIndexWriter();
        initIndexThreadPool();
    }

    @PreDestroy
    public void close() throws Exception {
        Exception last = null;
        while (!closeables.isEmpty()) {
            Closeable closeable = closeables.remove(closeables.size() - 1);
            try {
                closeable.close();
            } catch (Exception e) {
                last = e;
            }
        }
        if (last != null) throw last;
    }

    private void initAnalyzer() {
        analyzer = new StandardAnalyzer();
        closeables.add(analyzer);
    }

    private void initDirectory() throws IOException {
        if (config.path != null) {
            indexDirectory = FSDirectory.open(new File(config.path).toPath());
        }
        else {
            indexDirectory = new ByteBuffersDirectory();
        }
        closeables.add(indexDirectory);
    }

    private void initIndexWriter() throws IOException {
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);
        closeables.add(indexWriter);
    }

    private void initIndexThreadPool() {
        scheduler = Schedulers.newSingle(
                //Runtime.getRuntime().availableProcessors(), config.indexTaskQueueLimit,
                getClass().getSimpleName());
        closeables.add(scheduler::dispose);
    }

    public void deleteAll() {
        long seq = 0;
        try {
            seq = indexWriter.deleteAll();
        } catch (IOException e) {
            log.error("[DELETE_ALL]", e);
        }
        if (debugEventDeleteAll) {
            log.debug("[DELETE_ALL] -- {}", seq);
        }
        commit();
    }

    public void commit() {
        long seq = 0;
        try {
            seq = indexWriter.commit();
        } catch (IOException e) {
            log.error("[COMMIT]", e);
        }
        if (debugEventCommit) {
            log.debug("[COMMIT] -- {}", seq);
        }
    }

    public void indexDir(File dir) {
        if (config.excludeDirContains != null) {
            for (String token : config.excludeDirContains) {
                if (dir.toString().contains(token)) {
                    log.debug("[SKIP] -- {}", dir);
                    return;
                }
            }
        }
        log.debug("[INDEX_ALL] -- {}", dir);
        final File[] files = dir.listFiles();
        if (files == null || files.length == 0) return;
        boolean hasFile = false;
        for (File file : files) {
            if (file.isDirectory()) {
                schedule_indexAll(file);
            }
            else {
                schedule_index(file);
                hasFile = true;
            }
        }
        if (hasFile) schedule_commit();
    }

    public IndexSearcher searcher() throws IOException {
        IndexReader indexReader = DirectoryReader.open(indexWriter);
        return new IndexSearcher(indexReader);
    }

    private void schedule_indexAll(File dir) {
        scheduler.schedule(() -> indexDir(dir));
    }

    private void schedule_index(File source) {
        scheduler.schedule(() -> indexFile(source));
    }

    private void schedule_commit() {
        scheduler.schedule(() -> commit());
    }

    @ToString
    private class FileForIndex {

        private final File source;
        private final String name;
        private final String path;
        private final String ext;

        private final long lastModified;

        public FileForIndex(File source) {
            this.source = source;
            this.name = source.getName();
            this.path = source.toPath().toString();

            final int dot = name.lastIndexOf('.');
            this.ext = dot >= 0 ? name.substring(dot).toLowerCase() : "";

            this.lastModified = source.lastModified();
        }

        public boolean indexAllowed() {
            if (source.isFile()) {
                if (config.indexAllowedExts != null) {
                    for (String allowedExt : config.indexAllowedExts) {
                        if (name.endsWith(allowedExt)) return true;
                    }
                }
                if (config.indexAllowedRegex != null) {
                    return name.matches(config.indexAllowedRegex);
                }
            }
            return false;
        }

        public String readContent() {
            String _content = null;
            try {
                _content = Files.lines(source.toPath()).collect(limitingJoin(config.fileContentLimit));
            } catch (IOException e) {
                log.error("FileForIndex('{}') -- {}", path, e.toString());
            }
            return _content;
        }

        public Term asTerm() {
            return new Term("path", path);
        }

        public Document asDocument() {
            Document document = new Document();
            document.add(new StringField("name", name, Field.Store.YES));
            document.add(new StringField("path", path, Field.Store.YES));
            document.add(new StringField("ext", ext, Field.Store.NO));
            document.add(new LongPoint("lastModified", lastModified));

            String content = readContent();
            if (content == null) return null;
            document.add(new TextField("content", content, Field.Store.NO));

            return document;
        }
    }

    public void indexFile(File source) {
        FileForIndex fileForIndex = new FileForIndex(source);
        if (fileForIndex.indexAllowed()) {
            Term term = fileForIndex.asTerm();
            Document document = fileForIndex.asDocument();
            if (document != null) {
                long seq = 0;
                try {
                    seq = indexWriter.updateDocument(term, document);
                } catch (Exception e) {
                    log.error("[INDEX]", e);
                }
                if (debugEventIndex) {
                    final String content = debugEventIndex_Content ? document.get("content") : "";
                    log.debug("[INDEX] -- {}" +
                            "\r\n  - {}" +
                            "\r\n  - {}" +
                            "\r\n", seq, fileForIndex, content);
                }
            }
        }
    }

    // https://stackoverflow.com/questions/35803000/joining-strings-with-limit
    private static Collector<String, List<String>, String> limitingJoin(String delimiter, int limit, String ellipsis) {
        return Collector.of(
                ArrayList::new,
                (l, e) -> {
                    if (l.size() < limit) l.add(e);
                    else if (l.size() == limit) l.add(ellipsis);
                },
                (l1, l2) -> {
                    l1.addAll(l2.subList(0, Math.min(l2.size(), Math.max(0, limit - l1.size()))));
                    if (l1.size() == limit) l1.add(ellipsis);
                    return l1;
                },
                l -> String.join(delimiter, l)
        );
    }

    private static Collector<String, List<String>, String> limitingJoin(int limit) {
        return limitingJoin("", limit, "");
    }

    public static void main(String[] args) throws Exception {
        final LuceneIndexer luceneIndexer = new LuceneIndexer();
        luceneIndexer.init();
        try {
            runInConsole(luceneIndexer);
        } finally {
            luceneIndexer.close();
        }
    }

    private static int rows = 30;

    private static void runInConsole(LuceneIndexer luceneIndexer) throws IOException {
        final IndexSearcher searcher = luceneIndexer.searcher();
        System.out.println(">> START QUERYING HERE <<");
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String query = scanner.nextLine();
            System.out.println("["+query+"]");

            if (query.charAt(0) == '-') {
                String[] tokens = query.split(" ");
                for (int i = 0, tokensLength = tokens.length; i < tokensLength; i++) {
                    String token = tokens[i];
                    if (token.equalsIgnoreCase("-index")) {
                        File dir = new File("C:\\Users\\pk_th\\IdeaProjects");
                        if (i + 1 < tokensLength) {
                            token = tokens[++i];
                            File _dir = new File(token);
                            if (_dir.exists()) dir = _dir;
                        }
                        System.out.println("INDEXING | " + dir);
                        luceneIndexer.indexDir(dir);
                    } else if (token.equalsIgnoreCase("-delete")) {
                        luceneIndexer.deleteAll();
                    } else if (token.equalsIgnoreCase("-quit")) {
                        return;
                    }
                    else if (token.equalsIgnoreCase("-rows")) {
                        if (i + 1 < tokensLength) {
                            token = tokens[++i];
                            rows = Integer.parseInt(token);
                        }
                    }
                }

                continue;
            }

            try {
                //query = query.toLowerCase()+'*';
                luceneIndexer.queryContent(searcher, query, rows);
            } catch (Exception e) {
                System.out.println("ERROR | "+e.toString());
            }
        }
    }

    private void queryContent(IndexSearcher searcher, String s, int rows) throws Exception {
        QueryParser qp = new QueryParser("content", analyzer);
        Query query = qp.parse(s);
        TopDocs hits = searcher.search(query, rows);

        ScoreDoc[] scoreDocs = hits.scoreDocs;
        if (scoreDocs.length == 0) {
            System.out.println("[NO RESULTS]");
            return;
        }
        for (int i = 0; i < scoreDocs.length; i++) {
            ScoreDoc scoreDoc = scoreDocs[i];
            Document d = searcher.doc(scoreDoc.doc);
            print(i+1, d);
        }
    }

    private static void print(int index, Document d) {
        String path = d.get("path");
        System.out.printf("[%02d] | %s%n", index, path);
    }
}
