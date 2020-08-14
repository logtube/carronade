package io.github.logtube.carronade;

import com.jsoniter.JsonIterator;
import net.guoyk.eswire.ElasticWire;
import net.guoyk.eswire.ElasticWireCallback;
import net.guoyk.eswire.ElasticWireOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static String extractDocumentProject(byte[] bytes) throws IOException {
        JsonIterator iter = JsonIterator.parse(bytes);
        for (String field = iter.readObject(); field != null; field = iter.readObject()) {
            if ("project".equals(field)) {
                return iter.readString();
            } else {
                iter.skip();
            }
        }
        return null;
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        HashMap<String, Integer> counters = new HashMap<>();
        ElasticWireOptions options = new ElasticWireOptions();
        options.setDataDirs(new String[]{
                "/data01/data",
                "/data02/data",
                "/data03/data",
                "/data04/data",
                "/data05/data",
                "/data06/data",
                "/data07/data",
                "/data08/data",
        });
        ElasticWire elasticWire = new ElasticWire(options);
        elasticWire.export(args[0], new ElasticWireCallback() {

            private long progress = 0;

            private boolean sampled = false;

            @Override
            public boolean handleDocumentSource(byte[] bytes, long id, long total) {
                if (!this.sampled) {
                    LOGGER.info("sample = {}", new String(bytes));
                    this.sampled = true;
                }
                long newProgress = (long) ((double) id * 100 / (double) total);
                if (newProgress != this.progress) {
                    LOGGER.info("progress = {}%", newProgress);
                    this.progress = newProgress;
                }
                try {
                    String project = extractDocumentProject(bytes);
                    if (project == null) {
                        LOGGER.error("missing project field");
                        return false;
                    }
                    counters.put(project, counters.getOrDefault(project, 0) + 1);
                    return true;
                } catch (IOException e) {
                    LOGGER.error("failed to extract project", e);
                    return false;
                }
            }
        });
        LOGGER.info("aggregations = {}", counters);
    }

}
