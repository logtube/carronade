package io.github.logtube.carronade;

import com.jayway.jsonpath.JsonPath;
import net.guoyk.eswire.ElasticWire;
import net.guoyk.eswire.ElasticWireOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

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
        elasticWire.export(args[0], (bytes, id, total) -> {
            String project = JsonPath.read(new String(bytes), "$.project");
            if (project == null) {
                return false;
            }
            counters.put(project, counters.getOrDefault(project, 0) + 1);
            return true;
        });
        LOGGER.info("aggs = {}", counters);
    }

}
