package com.nexmo.jobs;

import com.nexmo.NexmoSbAppApplication;
import com.nexmo.entities.LogData;
import com.nexmo.mappers.LogDataLineMapper;
import com.nexmo.processors.LogDataItemProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.io.File;
import java.io.FilenameFilter;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    private static final Logger log = LoggerFactory.getLogger(BatchConfiguration.class);
    private static final int BATCH_SIZE = 5000;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

    @Autowired
    LogDataLineMapper logDataLineMapper;

    @Autowired
    FileVerificationSkipper skipPolicy;

    @Bean
    public ItemReader<LogData> logDataReader() {

        // Process only one file
        if (NexmoSbAppApplication.cliArgs.containsKey("file")) {
            FlatFileItemReader reader = new FlatFileItemReader();
            String filePath = NexmoSbAppApplication.cliArgs.get("file");
            log.info("Preparing to process file at {}", filePath);

            reader.setResource(new FileSystemResource(filePath));
            reader.setLineMapper(logDataLineMapper);
            return reader;
        } else if (NexmoSbAppApplication.cliArgs.containsKey("dir")) {
            String dirPath = NexmoSbAppApplication.cliArgs.get("dir");
            Resource[] resources = null;

            File dir = new File(dirPath);

            File[] csvFiles = dir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String filename) {
                    return filename.endsWith(".csv");
                }
            });

            if (csvFiles.length == 0) {
                System.out.println("No CSV files found in " + dirPath + "; nothing to do!");
                System.exit(0);
            }

            resources = new Resource[csvFiles.length];

            for (int i = 0; i < csvFiles.length; i++) {
                resources[i] = new FileSystemResource(csvFiles[0].getAbsolutePath());
            }

            MultiResourceItemReader<LogData> reader = new MultiResourceItemReader<>();

            reader.setResources(resources);
            FlatFileItemReader flatFileItemReader = new FlatFileItemReader<>();
            flatFileItemReader.setLineMapper(logDataLineMapper);
            reader.setDelegate(flatFileItemReader);
            return reader;
        } else {
            System.out.println("No dir or file in options map - fatal error!");
            System.exit(1);
        }

        return null;
    }

    @Bean
    public LogDataItemProcessor logDataItemProcessor() {
        return new LogDataItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<LogData> logDataWriter() {
        JdbcBatchItemWriter<LogData> writer = new JdbcBatchItemWriter<LogData>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<LogData>());
        writer.setSql("INSERT INTO log_data (message_id, timestamp, account_id, gateway_id, country, status, price, cost) " +
                "VALUES (:messageId, :timestamp, :accountId, :gatewayId, :country, :status, :price, :cost)");
        writer.setDataSource(dataSource);
        return writer;
    }

    @Bean
    public Job importRecordJob(JobCompletionNotificationListener listener) {
        return jobBuilderFactory.get("importRecordJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1())
                .end()
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<LogData, LogData>chunk(BATCH_SIZE)
                .reader(logDataReader()).faultTolerant().skipPolicy(skipPolicy)
                .processor(logDataItemProcessor())
                .writer(logDataWriter())
                .build();
    }
}