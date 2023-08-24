package augustus.personal.lucene;

import org.apache.lucene.document.*;
import org.apache.lucene.index.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Created by thooaug on 24/8/2023.
 */
public record FileForIndex(File source, String name, String path, String ext, long lastModified) {

    public FileForIndex {
    }

    public boolean isDirectory() {
        return source.isDirectory();
    }

    public boolean isFile() {
        return source.isFile();
    }

    public static FileForIndex from(File source) {
        String name = source.getName();
        String path = source.toPath().toString();
        final int dot = name.lastIndexOf('.');
        String ext = dot >= 0 ? name.substring(dot).toLowerCase() : "";
        long lastModified = source.lastModified();
        return new FileForIndex(source, name, path, ext, lastModified);
    }

    public String readContent(int limit) {
        try {
            return FileUtils.limitByLength(source, limit);
        } catch (IOException | UncheckedIOException e) {
            Logger logger = LoggerFactory.getLogger(FileForIndex.class);
            logger.error("FileForIndex('{}') -- {}", path, e.toString());
            return null;
        }
    }

    public Term asTerm() {
        return new Term("path", path);
    }

    public Document asDocument(int limit) {
        Document document = new Document();
        document.add(new StringField("name", name, Field.Store.YES));
        document.add(new StringField("path", path, Field.Store.YES));
        document.add(new StringField("ext", ext, Field.Store.NO));
        document.add(new LongPoint("lastModified", lastModified));

        String content = readContent(limit);
        if (content == null) return null;
        document.add(new TextField("content", content, Field.Store.NO));

        return document;
    }
}
