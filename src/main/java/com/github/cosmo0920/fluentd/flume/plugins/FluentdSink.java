package com.github.cosmo0920.fluentd.flume.plugins;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.annotations.VisibleForTesting;

public class FluentdSink extends AbstractSink implements Configurable {
	private static final Logger logger = LoggerFactory.getLogger(FluentdSink.class);

	private static final int DEFAULT_PORT = 24224;
	private static final String DEFAULT_HOST = "localhost";
	private static final String DEFAULT_TAG = "flume.fluentd.sink";

	private String hostname;
	private int port;
	private String tag;
	private String format;
	private String backupDir;
	private int batchSize = 1000;

	@VisibleForTesting
	public FluencyPublisher publisher;

	private CounterGroup counterGroup;

	public FluentdSink() {
		counterGroup = new CounterGroup();
	}

	public void configure(Context context) {
		hostname = context.getString("hostname");
		String portStr = context.getString("port");
		tag = context.getString("tag");
		format = context.getString("format");
		backupDir = context.getString("backupDir");
		String batchSizeStr = context.getString("batchSize");

		if (portStr != null) {
			port = Integer.parseInt(portStr);
		} else {
			port = DEFAULT_PORT;
		}

		if (hostname == null) {
			hostname = DEFAULT_HOST;
		}

		if (tag == null) {
			tag = DEFAULT_TAG;
		}

		if (backupDir == null) {
			logger.warn("Unable to find backupDir in conf. Log lost may happen.");
		}
		
		if (batchSizeStr != null && !batchSizeStr.isEmpty()) {
			batchSize = Integer.parseInt(batchSizeStr);
			if (batchSize <= 0) {
				throw new IllegalArgumentException("Property 'batchSize' must be greater than zero.");
			}
		}

		Preconditions.checkState(hostname != null, "No hostname specified");
		Preconditions.checkState(tag != null, "No tag specified");
		Preconditions.checkState(format != null, "No format specified");

		publisher = new FluencyPublisher(tag, format);
	}

	@Override
	public void start() {
		logger.info("Property hostname={}", hostname);
		logger.info("Property port={}", port);
		logger.info("Property tag={}", tag);
		logger.info("Property batchSize={}", batchSize);
		logger.info("Property backupDir={}", backupDir);
		logger.info("Fluentd sink starting");

		try {
			if (backupDir != null) {
				publisher.setup(hostname, port, backupDir);
			} else {
				publisher.setup(hostname, port);
			}
		} catch (IOException e) {
			logger.error("Unable to create Fluentd logger using hostname:"
						 + hostname + " port:" + port + ". Exception follows.", e);

			publisher.close();
			throw new RuntimeException(e);
		}

		super.start();

		logger.debug("Fluentd sink {} started", this.getName());
	}

	@Override
	public void stop() {
		logger.info("Fluentd sink {} stopping", this.getName());

		publisher.close();

		super.stop();

		logger.debug("Fluentd sink {} stopped. Metrics:{}", this.getName(), counterGroup);
	}

	@Override
	public Status process() throws EventDeliveryException {
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();

		try {
			transaction.begin();

			int i = 0;
			for (; i < batchSize; i++) {
				Event event = channel.take();
	
				if (event == null) {
					break;
				} else {
					publisher.publish(event);
				}
			}

			transaction.commit();

			if (i > 0) {
				counterGroup.addAndGet("event.fluentd", (long) i);
				return Status.READY;
			} else {
				counterGroup.incrementAndGet("event.empty");
				return Status.BACKOFF;
			}
		} catch (ChannelException e) {
			transaction.rollback();
			logger.error("Unable to get event from channel. Exception follows.", e);
			return Status.BACKOFF;
		} catch (IOException e) {
			transaction.rollback();
			logger.error("Unable to communicate with Fluentd. Exception follows.", e);
			return Status.BACKOFF;
		} catch (Throwable e) {
			transaction.rollback();
			logger.error("Unable to process event. Exception follows.", e);
			if (e instanceof Error) {
				throw (Error) e;
			} else if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new EventDeliveryException(e);
			}
		} finally {
			transaction.close();
		}
	}
}
