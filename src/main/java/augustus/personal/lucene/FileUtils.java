package augustus.personal.lucene;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Created by thooaug on 24/8/2023.
 */
class FileUtils {

    static String limitByLength(File source, int length) throws IOException {
        return limitByLength(source, length, Charset.defaultCharset());
    }

    static String limitByLength(File source, int length, Charset charset) throws IOException {
        try (Stream<String> lines = Files.lines(source.toPath(), charset)) {
            return limitByLength(lines, length);
        }
    }

    static String limitByLength(Stream<String> src, int length) {
        StringBuilder buf = new StringBuilder(length);
        //Predicate<String> LESS = s -> buf.length() + s.length() < length;
        Predicate<String> MORE = s -> buf.length() < length;
        src.takeWhile(MORE).forEach(s -> buf.append("\r\n").append(s));
        return buf.length() > length ? buf.substring(0, length) : buf.toString();
    }
}
