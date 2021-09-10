
package org.apache.kafka.streams.examples.harrycount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Suppressed;
// import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
// import org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
// import org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
// import org.apache.kafka.streams.kstream.internals.suppress.BufferFullStrategy.SHUT_DOWN;

import org.apache.kafka.streams.KeyValue;


import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.io.FileInputStream;
import java.io.IOException;

public final class HarryCount {

    public static final String INPUT_TOPIC = "streams-harrycount-input";
    public static final String OUTPUT_TOPIC = "streams-harrycount-output";

    static Properties getStreamsConfig(final String[] args) throws IOException {
		
		final Properties props = new Properties();
		if (args != null && args.length > 0) {
			try (final FileInputStream fis = new FileInputStream(args[0])) {
				props.load(fis);
			}
			if (args.length > 1) {
				System.out.println("Warning: Some command line arguments were ignored. This demo only accepts an optional configuration file.");
			}
		}
        
		props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-harrycount");
		props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		// setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
		// Note: To re-run the demo, you need to use the offset reset tool:
		// https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
		props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return props;
	}

    static void createHarryCountStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);
        final KStream<String, Long> counts = source				
			.flatMapValues(value -> {
				System.out.println("value: " + value);
				return Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" "));
			})
			.groupBy((key, value) -> {
				System.out.println("g value: " + value);
				System.out.println("g key: " + key);
				return value;					
			})
			// .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
			.windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
			.count()
			.toStream()			
			.map((key, value) -> {
				System.out.println("m time: " + key.window());
				System.out.println("m value: " + value);
				System.out.println("m key: " + key.key());
				return KeyValue.pair(key.key(), value);})
			.filter((key, value) -> {
				System.out.println("f value: " + value);
				System.out.println("f key: " + key);
				return "harry".equals(key);					
			});
			// .filter((key, value) -> "harry".equals(value));

        // need to override value serde to Long type
        counts.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    public static void main(final String[] args) {
        //final Properties props = getStreamsConfig();
		
		try{
			
			final Properties props = getStreamsConfig(args);
			final StreamsBuilder builder = new StreamsBuilder();
			createHarryCountStream(builder);
			final KafkaStreams streams = new KafkaStreams(builder.build(), props);
			final CountDownLatch latch = new CountDownLatch(1);

			// attach shutdown handler to catch control-c
			Runtime.getRuntime().addShutdownHook(new Thread("streams-harrycount-shutdown-hook") {
				@Override
				public void run() {
					streams.close();
					latch.countDown();
				}
			});
            streams.start();
            latch.await();
		
		}
		catch(IOException e) {
			e.printStackTrace();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}

