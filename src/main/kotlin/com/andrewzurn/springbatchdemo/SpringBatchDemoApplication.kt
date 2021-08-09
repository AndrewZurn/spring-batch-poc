package com.andrewzurn.springbatchdemo

import com.datastax.oss.driver.api.core.CqlSessionBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal
import com.datastax.oss.driver.api.querybuilder.relation.Relation
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobInstance
import org.springframework.batch.core.JobParameter
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.explore.JobExplorer
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.item.kafka.KafkaItemReader
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.data.cassandra.core.CassandraTemplate
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import java.util.Properties
import java.util.UUID

// great example: https://github.com/spring-tips/kafka-and-spring-batch
@EnableBatchProcessing
@SpringBootApplication
class SpringBatchDemoApplication {

    private val log = LoggerFactory.getLogger(this.javaClass)

    @Autowired
    private lateinit var kafkaProperties: KafkaProperties

    @Value("\${spring.kafka.template.default-topic}")
    lateinit var defaultTopic: String

    @Bean
    fun job(
        jobBuilderFactory: JobBuilderFactory,
        stepBuilderFactory: StepBuilderFactory,
        reader: KafkaItemReader<String, String>,
        cassandraTemplate: CassandraTemplate,
    ): Job {
        val step1 = stepBuilderFactory
            .get("step1")
            .tasklet { contribution, chunkContext ->
                val userId = UUID.fromString(chunkContext.stepContext.stepExecution.jobParameters.getString("userId"))
                cassandraTemplate.execute(
                    QueryBuilder
                        .selectFrom(System.getenv("KEYSPACE"), System.getenv("TABLE"))
                        .json()
                        .columns(listOf("system_time", "display_time", "value"))
                        .where(Relation.column("patient_id").isEqualTo(literal(userId)))
                        .limit(1)
                        .build()
                ).forEach { row -> log.info(row.formattedContents) }
                RepeatStatus.continueIf(false)
            }
            .build()

        return jobBuilderFactory.get("user-export")
            .incrementer(RunIdIncrementer())
            .start(step1)
            .build()
    }

    @Bean
    @StepScope
    fun kafkaItemReader(): KafkaItemReader<String, String> {
        val props = Properties()
        props.putAll(kafkaProperties.buildConsumerProperties())

        return KafkaItemReaderBuilder<String, String>()
            .partitions(0)
            .consumerProperties(props)
            .name("user-export-reader")
            .saveState(true)
            .topic(defaultTopic)
            .build()
    }

    @Bean
    fun cassandraTemplate(): CassandraTemplate = CassandraTemplate(CqlSessionBuilder().build())
}

@RestController
class ExportController {

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Autowired
    private lateinit var job: Job

    @Autowired
    private lateinit var jobLauncher: JobLauncher

    @Autowired
    private lateinit var jobExplorer: JobExplorer

    @PostMapping("/users/{userId}/export")
    fun exportUserData(@PathVariable userId: String): JobInstance {
        val jobParams = mapOf("userId" to JobParameter(userId), "timestamp" to JobParameter(System.currentTimeMillis()))
        val something = jobLauncher.run(job, JobParameters(jobParams))
        return something.jobInstance
    }

    @GetMapping("/users/{userId}/export/{jobId}")
    fun getJobStatus(@PathVariable userId: String, @PathVariable jobId: Long): ResponseEntity<JobExecution> {
        val jobStatus = jobExplorer.getJobInstance(jobId)?.let { jobExplorer.getLastJobExecution(it) }
        return jobStatus?.let { ResponseEntity.ok(it) } ?: ResponseEntity.notFound().build()
    }
}

fun main(args: Array<String>) {
    runApplication<SpringBatchDemoApplication>(*args)
}
