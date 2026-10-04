package com.tailoredbrands.otd.migration.reconciler;

import com.tailoredbrands.otd.migration.common.jms.JmsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jms.JmsAutoConfiguration;
import org.springframework.boot.autoconfigure.jms.artemis.ArtemisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Reconciler: compares a time window of orders on the legacy side (EMS audit queue browse or an
 * export file) with the Pub/Sub side (BigQuery {@code otd.order_events} or a JSONL file).
 * Runs as a CLI, a Kubernetes CronJob or a Cloud Run Job ({@code deploy/cloudrun/job.yaml}).
 *
 * <pre>
 *   java -jar reconciler.jar --from=2026-10-03T00:00:00Z --to=2026-10-04T00:00:00Z \
 *        --legacyQueue=TB.ORDERS.AUDIT --bigQueryDataset=otd --phase=SHADOW --out=/tmp/recon
 * </pre>
 * Exit codes: 0 ok, 1 error, 2 differences found (with {@code --failOnDiff=true}).
 */
@SpringBootApplication(exclude = {JmsAutoConfiguration.class, ArtemisAutoConfiguration.class})
@EnableConfigurationProperties(JmsSettings.class)
public class ReconcilerApplication {

    private static final Logger log = LoggerFactory.getLogger(ReconcilerApplication.class);

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(ReconcilerApplication.class, args)));
    }

    @Bean
    public ReconcilerRunner reconcilerRunner(JmsSettings jmsSettings) {
        return new ReconcilerRunner(jmsSettings);
    }

    @Bean
    public ReconcilerCommand reconcilerCommand(ReconcilerRunner runner) {
        return new ReconcilerCommand(runner);
    }

    /** Parses the command line, runs once, exposes the exit code to {@link SpringApplication#exit}. */
    public static class ReconcilerCommand implements ApplicationRunner, ExitCodeGenerator {

        private final ReconcilerRunner runner;
        private volatile int exitCode = ReconcilerRunner.EXIT_ERROR;

        public ReconcilerCommand(ReconcilerRunner runner) {
            this.runner = runner;
        }

        @Override
        public void run(ApplicationArguments args) {
            ReconcilerOptions options;
            try {
                options = ReconcilerOptions.parse(args.getSourceArgs());
            } catch (IllegalArgumentException e) {
                log.error("Bad arguments: {}", e.getMessage());
                exitCode = ReconcilerRunner.EXIT_ERROR;
                return;
            }
            exitCode = runner.run(options);
        }

        @Override
        public int getExitCode() {
            return exitCode;
        }
    }
}
