package augustus.personal.lucene;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by Augustus Thoo on 4/24/2021
 */
@Slf4j
public class LuceneIndexer {

    private static final boolean debugEventCommit = true;
    private static final boolean debugEventDeleteAll = true;

    @Setter
    public static class Config {
        private String path = "lucene";
    }

    private Config config = new Config();

    private List<Closeable> closeables;
    private Analyzer analyzer;
    private Directory indexDirectory;
    private IndexWriter indexWriter;

    public void setConfig(Config config) {
        this.config = config;
    }

    public void init() throws Exception {
        closeables = new ArrayList<>();
        initAnalyzer();
        initDirectory();
        initIndexWriter();
    }

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

    public IndexSearcher searcher() throws IOException {
        IndexReader indexReader = DirectoryReader.open(indexWriter);
        return new IndexSearcher(indexReader);
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
                    if (token.equalsIgnoreCase("-delete")) {
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
