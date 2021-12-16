package com.github.mmolimar.kafka.connect.fs.file.reader;

import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

/**
 * @author Alex.Sun
 * @created 2021-12-16 10:15
 */
public class PatternFileReader extends AbstractFileReader<PatternFileReader.LineRecord> {

    private static final String FILE_READER_LOG = FILE_READER_PREFIX + "pattern.";
    private static final String FILE_READER_LOG_PATTERN = FILE_READER_LOG + "pattern";
    public static final String FILE_READER_LOG_ENCODING = FILE_READER_LOG + "encoding";
    private static final String FILE_READER_LOG_COMPRESSION = FILE_READER_LOG + "compression.type";

    private static final Map<String, Pattern> PRE_DEFINED_PATTERNS;
    private static final Pattern GROUP_NAME_PATTERN = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");

    static {
        Map<String, Pattern> map = new CaseInsensitiveMap();
        map.put("RFC3164", Pattern.compile("^(<(?<priority>\\d+)>)?(?<date>([a-zA-Z]{3}\\s+\\d+\\s+\\d+:\\d+:\\d+)|([0-9T:.Z-]+))\\s+(?<host>\\S+)\\s+((?<tag>[^\\[\\s\\]]+)(\\[(?<procid>\\d+)\\])?:)*\\s*(?<message>.+)$"));
        map.put("RFC5424", Pattern.compile("^<(?<priority>\\d+)>(?<version>\\d{1,3})\\s*(?<date>[0-9:+-TZ]+)\\s*(?<host>\\S+)\\s*(?<appname>\\S+)\\s*(?<procid>\\S+)\\s*(?<msgid>\\S+)\\s*(?<structureddata>(-|\\[.+\\]))\\s*(?<message>.+)$"));
        PRE_DEFINED_PATTERNS = Collections.unmodifiableMap(map);
    }

    private final TextFileReader inner;
    private Pattern pattern;
    private Schema schema;

    @SneakyThrows
    public PatternFileReader(FileSystem fs, Path filePath, Map<String, Object> config) {
        super(fs, filePath, new LineToStruct(), config);

        config.put(TextFileReader.FILE_READER_TEXT_ENCODING, config.get(FILE_READER_LOG_ENCODING));
        config.put(TextFileReader.FILE_READER_TEXT_RECORD_PER_LINE, true);
        config.put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, config.get(FILE_READER_LOG_COMPRESSION));
        config.put(TextFileReader.FILE_READER_TEXT_COMPRESSION_CONCATENATED, true);

        this.inner = new TextFileReader(fs, filePath, config);
    }

    @Override
    protected void configure(Map<String, String> config) {
        String keyOrPattern = config.get(FILE_READER_LOG_PATTERN);
        if (StringUtils.isNotEmpty(keyOrPattern)) {
            this.pattern = PRE_DEFINED_PATTERNS.get(keyOrPattern);
            if (this.pattern == null) {
                this.pattern = Pattern.compile(keyOrPattern);
            }
        } else {
            throw new ConfigException("必须设置pattern");
        }

        Matcher matcher = GROUP_NAME_PATTERN.matcher(this.pattern.pattern());
        SchemaBuilder builder = SchemaBuilder.struct().optional();
        while (matcher.find()) {
            builder.field(matcher.group(1), Schema.OPTIONAL_STRING_SCHEMA);
        }
        this.schema = builder.build();
        if (CollectionUtils.isEmpty(this.schema.fields())) {
            throw new ConfigException("pattern中未找到提取字段名");
        }
    }

    @Override
    protected LineRecord nextRecord() throws IOException {
        String line = inner.nextRecord().getValue();
        Matcher matcher = this.pattern.matcher(line);

        Map<String, String> value = null;
        if (matcher.find()) {
            value = schema.fields().stream().collect(Collectors.toMap(field -> field.name(), field -> matcher.group(field.name())));
        } else {
            log.warn("Pattern not match: " + line);
        }
        return new LineRecord(schema, value);
    }

    @Override
    protected boolean hasNextRecord() throws IOException {
        return inner.hasNextRecord();
    }

    @Override
    protected void seekFile(long offset) throws IOException {
        inner.seekFile(offset);
    }

    @Override
    protected boolean isClosed() {
        return inner.isClosed();
    }

    @Override
    public long currentOffset() {
        return inner.currentOffset();
    }

    @Override
    public void incrementOffset() {
        inner.incrementOffset();
    }

    @Override
    public void setOffset(long offset) {
        inner.setOffset(offset);
    }

    @Override
    public void remove() {
        inner.remove();
    }

    @Override
    public void forEachRemaining(Consumer<? super Struct> action) {
        inner.forEachRemaining(action);
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }

    //
    static class LineRecord {
        private final Schema schema;
        private final Map<String, String> value;

        public LineRecord(Schema schema, Map<String, String> value) {
            this.schema = schema;
            this.value = value;
        }

        public boolean hasValue() {
            return value != null && !value.isEmpty();
        }
    }

    static class LineToStruct implements ReaderAdapter<LineRecord> {

        @Override
        public Struct apply(LineRecord lineRecord) {
            if (!lineRecord.hasValue()) return null;

            Struct struct = new Struct(lineRecord.schema);
            lineRecord.schema.fields().stream().forEach(field -> {
                struct.put(field, lineRecord.value.get(field.name()));
            });
            return struct;
        }
    }
}
