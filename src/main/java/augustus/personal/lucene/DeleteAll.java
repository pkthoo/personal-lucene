package augustus.personal.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Created by thooaug on 10/9/2023.
 */
public class DeleteAll {

    private static final Logger logger = LoggerFactory.getLogger(DeleteAll.class);

    public static void main(String[] args) throws IOException {
        doDeleteAll(Path.of("lucene"));
    }

    public static void doDeleteAll(Path path) throws IOException {
        logger.info("[DELETING INDEX AT {}] ...", path);
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        try (Directory indexDirectory = FSDirectory.open(path)) {
            try (IndexWriter indexWriter = new IndexWriter(indexDirectory, indexWriterConfig)) {
                indexWriter.deleteAll();
                logger.info("[DELETED INDEX AT {}] ...", path);
            }
        }
    }
}
