package com.linkedin.camus.shopify;


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

/**
 * Log4j wrapper class that also outputs to stdout.
 * Due to Yarn/Splunk set up this is a cheap and cheerful solution to pull out important logs from mappers.
 * These normally go to syslog, but we also want to publish to stdout so that they are picked up by our Splunk setup.
 */
public class CamusLogger {
    private Logger underlying;
    private String forClass;


    public CamusLogger(Class clazz) {
        this.forClass = clazz.getName();
        this.underlying = LogManager.getLogger(this.forClass);
    }

    public CamusLogger(Logger logger) {
        this.forClass = logger.getName();
        this.underlying = logger;
    }

    private void println(String message, String level) {
        System.out.println(
                String.format("%s %s %s %s",
                new DateTime(System.currentTimeMillis()).toString(), level, this.forClass, message));

    }

    public void debug(String message) {
        this.underlying.debug(message);
    }

    public void info(String message) {
        println(message, "INFO");
        this.underlying.info(message);
    }

    public void warn(String message) {
        println(message, "WARN");
        this.underlying.warn(message);
    }

    public void error(String message) {
        println(message, "ERROR");
        this.underlying.error(message);
    }
}
