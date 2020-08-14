package io.github.logtube.carronade;

import net.guoyk.eswire.ElasticWire;
import net.guoyk.eswire.ElasticWireOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        String index = args[0];

        Workspace workspace = new Workspace(Paths.get(System.getProperty("workspace"), index).toString());
        LOGGER.info("使用工作空间: {}", workspace.getWorkspace());

        String[] dataDirs = Arrays.stream(System.getProperty("es.dataDirs")
                .split(","))
                .map(String::trim)
                .toArray(String[]::new);
        LOGGER.info("本机 ElasticSearch 数据目录: {}", (Object) dataDirs);

        ElasticWireOptions options = new ElasticWireOptions();
        options.setDataDirs(dataDirs);
        ElasticWire elasticWire = new ElasticWire(options);
        elasticWire.export(index, workspace);
        LOGGER.info("完成导出: {}", workspace.getCounters());
        elasticWire.close();
        workspace.close();
    }

}
