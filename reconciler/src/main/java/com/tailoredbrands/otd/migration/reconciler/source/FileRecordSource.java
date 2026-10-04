package com.tailoredbrands.otd.migration.reconciler.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tailoredbrands.otd.migration.common.json.JsonMappers;
import com.tailoredbrands.otd.migration.common.xml.LegacyXmlMapper;
import com.tailoredbrands.otd.migration.reconciler.model.SideRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads either side from a file (used for the legacy export and for offline / unit-test runs):
 * <ul>
 *   <li><b>CSV</b> with a header row: {@code orderId,totalAmount,lineCount,timestamp,messageId}
 *       (snake_case or legacy names also accepted).</li>
 *   <li><b>JSON lines</b> (one object per line) or a JSON array: canonical {@code OrderEvent}s
 *       (with an {@code order} object), flattened BigQuery rows, or {@code {"xml": "<Order>…", "messageId": …}}
 *       objects holding raw legacy XML.</li>
 * </ul>
 * Rows whose timestamp falls outside the window are ignored. Unparseable lines are logged and skipped.
 */
public class FileRecordSource implements RecordSource {

    private static final Logger log = LoggerFactory.getLogger(FileRecordSource.class);

    private final Path path;
    private final String side;
    private final ObjectMapper json = JsonMappers.canonical();
    private final LegacyXmlMapper xmlMapper = new LegacyXmlMapper();

    public FileRecordSource(Path path, String side) {
        this.path = path;
        this.side = side;
    }

    @Override
    public String describe() {
        return side + " file " + path;
    }

    @Override
    public List<SideRecord> read(Instant from, Instant to) throws IOException {
        String content = Files.readString(path, StandardCharsets.UTF_8);
        List<SideRecord> all = looksLikeJson(content) ? readJson(content) : readCsv(content);
        List<SideRecord> inWindow = new ArrayList<>();
        for (SideRecord r : all) {
            if (r.orderId() == null || r.orderId().isBlank()) {
                log.warn("{}: skipping record without orderId: {}", describe(), r);
                continue;
            }
            if (r.inWindow(from, to)) {
                inWindow.add(r);
            }
        }
        log.info("{}: {} records, {} in window [{}, {})", describe(), all.size(), inWindow.size(), from, to);
        return inWindow;
    }

    private static boolean looksLikeJson(String content) {
        String trimmed = content.stripLeading();
        return trimmed.startsWith("{") || trimmed.startsWith("[");
    }

    private List<SideRecord> readJson(String content) throws IOException {
        List<SideRecord> out = new ArrayList<>();
        String trimmed = content.stripLeading();
        if (trimmed.startsWith("[")) {
            JsonNode array = json.readTree(trimmed);
            for (JsonNode node : array) {
                add(out, node, -1);
            }
            return out;
        }
        int lineNo = 0;
        for (String line : content.split("\\r?\\n")) {
            lineNo++;
            if (line.isBlank()) {
                continue;
            }
            try {
                add(out, json.readTree(line), lineNo);
            } catch (IOException | RuntimeException e) {
                log.warn("{}: skipping line {}: {}", describe(), lineNo, e.getMessage());
            }
        }
        return out;
    }

    private void add(List<SideRecord> out, JsonNode node, int lineNo) {
        try {
            out.add(RecordParsers.fromJson(node, xmlMapper));
        } catch (RuntimeException e) {
            log.warn("{}: skipping record {}: {}", describe(), lineNo < 0 ? "" : "at line " + lineNo, e.getMessage());
        }
    }

    private List<SideRecord> readCsv(String content) {
        List<SideRecord> out = new ArrayList<>();
        List<String> header = null;
        int lineNo = 0;
        for (String line : content.split("\\r?\\n")) {
            lineNo++;
            if (line.isBlank()) {
                continue;
            }
            List<String> cells = RecordParsers.splitCsv(line);
            if (header == null) {
                header = new ArrayList<>();
                for (String h : cells) {
                    header.add(RecordParsers.normalizeHeader(h));
                }
                continue;
            }
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < header.size() && i < cells.size(); i++) {
                row.put(header.get(i), cells.get(i));
            }
            try {
                out.add(RecordParsers.fromRow(row));
            } catch (RuntimeException e) {
                log.warn("{}: skipping CSV line {}: {}", describe(), lineNo, e.getMessage());
            }
        }
        return out;
    }
}
