package augustus.personal.lucene;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * Created by thooaug on 24/8/2023.
 */
@Slf4j
public class FileNameIndexer {

    public static void main(String[] args) throws Exception {
        log.info("START INDEXING NOW");
        FileNameIndexer indexer = new FileNameIndexer("lucene");
        try {
            File dir = new File(args[0]);
            indexer.indexFiles(dir);
        } finally {
            indexer.close();
        }
        log.info("DONE INDEXING");
    }

    int limit = 2097152;
    final StandardAnalyzer analyzer;
    final FSDirectory indexDirectory;

    String[] indexAllowedExts = new String[]{
            ".java", ".xml", ".yml", ".properties"
    };

    String[] excludeDirContains = new String[] {
            "artifactory", ".idea", "apache-ignite-src", "dbeaver", ".git", "target"
    };

    String indexAllowedRegex = null;

    public FileNameIndexer(String indexPath) throws IOException {
        analyzer = new StandardAnalyzer();
        indexDirectory = FSDirectory.open(Paths.get(indexPath));
    }

    public void close() throws IOException {
        analyzer.close();
        indexDirectory.close();
    }

    public void indexFiles(File dir) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        try (IndexWriter indexWriter = new IndexWriter(indexDirectory, config)) {
            indexFilesRecursively(indexWriter, dir);
            indexWriter.commit();
        }
    }

    private void indexFilesRecursively(IndexWriter indexWriter, File file) throws IOException {
        FileForIndex source = FileForIndex.from(file);
        if (!indexAllowed(source)) return;
        if (file.isDirectory()) {
            log.debug("[INDEX_DIR ] -- {}", file.getAbsolutePath());
            File[] files = file.listFiles();
            if (files != null) {
                for (File subFile : files) {
                    indexFilesRecursively(indexWriter, subFile);
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
            log.debug("[INDEX_FILE] -- {}", source);
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
                        log.info("[SKIP] -- {}", source.path());
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }
}
