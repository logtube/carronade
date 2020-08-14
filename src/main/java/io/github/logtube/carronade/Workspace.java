package io.github.logtube.carronade;

import com.jsoniter.JsonIterator;
import net.guoyk.eswire.ElasticWireCallback;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

public class Workspace implements ElasticWireCallback, Closeable, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Workspace.class);

    private final String workspace;

    private final HashMap<String, AtomicLong> counters = new HashMap<>();

    private final HashMap<String, FileOutputStream> fileOutputStreams;

    private final HashMap<String, GZIPOutputStream> gzipOutputStreams;

    private boolean sampled = false;

    private long progress = 0;

    public Workspace(String workspace) throws IOException {
        FileUtils.deleteDirectory(new File(workspace));
        FileUtils.forceMkdir(new File(workspace));
        this.workspace = workspace;
        this.fileOutputStreams = new HashMap<>();
        this.gzipOutputStreams = new HashMap<>();
    }

    public String getWorkspace() {
        return this.workspace;
    }

    public HashMap<String, AtomicLong> getCounters() {
        return counters;
    }

    private String extractDocumentProject(byte[] bytes) {
        JsonIterator iter = JsonIterator.parse(bytes);
        try {
            for (String field = iter.readObject(); field != null; field = iter.readObject()) {
                if ("project".equals(field)) {
                    return iter.readString();
                } else {
                    iter.skip();
                }
            }
        } catch (IOException e) {
            return null;
        }
        return null;
    }

    @Override
    public boolean handleDocumentSource(byte[] bytes, long id, long total) {
        // sample
        if (!this.sampled) {
            LOGGER.info("sample = {}", new String(bytes));
            this.sampled = true;
        }
        // progress
        long progress = (long) ((double) id * 100 / (double) total);
        if (progress != this.progress) {
            LOGGER.info("progress = {}%", progress);
            this.progress = progress;
        }
        // project
        String project = extractDocumentProject(bytes);
        if (project == null) {
            LOGGER.error("missing project field or invalid json: {}", new String(bytes));
            return false;
        }
        // get outputstream
        GZIPOutputStream gzipOutputStream = this.gzipOutputStreams.get(project);
        if (gzipOutputStream == null) {
            try {
                FileOutputStream fileOutputStream = new FileOutputStream(Paths.get(this.workspace, project + Constants.EXT_NDJSON_GZ).toFile());
                gzipOutputStream = new GZIPOutputStream(fileOutputStream);
                this.fileOutputStreams.put(project, fileOutputStream);
                this.gzipOutputStreams.put(project, gzipOutputStream);
            } catch (IOException e) {
                LOGGER.error("failed to create gzip file", e);
                return false;
            }
        }
        // count
        AtomicLong counter = this.counters.get(project);
        if (counter == null) {
            counter = new AtomicLong();
            this.counters.put(project, counter);
        }
        counter.incrementAndGet();
        // write document
        try {
            gzipOutputStream.write(bytes);
            gzipOutputStream.write('\n');
        } catch (IOException e) {
            LOGGER.error("failed to write gzip file", e);
            return false;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        for (GZIPOutputStream value : this.gzipOutputStreams.values()) {
            value.close();
        }
        for (FileOutputStream value : this.fileOutputStreams.values()) {
            value.close();
        }
        this.gzipOutputStreams.clear();
        this.fileOutputStreams.clear();
        this.counters.clear();
    }
}
