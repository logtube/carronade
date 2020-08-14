package io.github.logtube.carronade;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.StorageClass;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.transfer.TransferManagerConfiguration;
import com.qcloud.cos.transfer.Upload;
import net.guoyk.eswire.ElasticWire;
import net.guoyk.eswire.ElasticWireOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

        List<String> projects = new ArrayList<>(workspace.getCounters().keySet());
        LOGGER.info("所有项目: {}", projects);

        elasticWire.close();
        workspace.close();

        COSCredentials cosCredentials = new BasicCOSCredentials(System.getProperty("cos.secretId"), System.getProperty("cos.secretKey"));
        Region region = new Region(System.getProperty("cos.region"));
        COSClient client = new COSClient(cosCredentials, new ClientConfig(region));

        String bucketName = System.getProperty("cos.bucket");

        ExecutorService threadPool = Executors.newFixedThreadPool(16);
        TransferManager transferManager = new TransferManager(client, threadPool);
        TransferManagerConfiguration transferManagerConfiguration = new TransferManagerConfiguration();
        transferManagerConfiguration.setMultipartUploadThreshold(1024 * 1024 * 1024);
        transferManagerConfiguration.setMinimumUploadPartSize(512 * 1024 * 1024);

        for (String project : projects) {
            String key = index + "/" + project + Constants.EXT_NDJSON_GZ;
            File localFile = Paths.get(workspace.getWorkspace(), project + Constants.EXT_NDJSON_GZ).toFile();
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, localFile);
            putObjectRequest.setStorageClass(StorageClass.Standard_IA);

            Upload upload = transferManager.upload(putObjectRequest);
            upload.waitForCompletion();
            LOGGER.info("上传完成: {}", key);
        }

        transferManager.shutdownNow();
    }

}
