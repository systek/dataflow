package no.systek.dataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ContextSwitcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContextSwitcher.class);

    /**
     * Simple wrapper which copies over the context (MDC and correlation) to the executing thread and
     * logs uncaught exceptions
     */
    public static Runnable wrap(Runnable in, Supplier<String> correlationIdProvider, Consumer<String> correlationIdSetter) {
        final Optional<Map<String, String>> context = Optional.ofNullable(MDC.getCopyOfContextMap());
        final Optional<String> korrelasjonsId = Optional.ofNullable(correlationIdProvider.get());
        return () -> {
            Optional<Map<String, String>> contextBackup = Optional.ofNullable(MDC.getCopyOfContextMap());
            final Optional<String> backupKorrelasjonsId = Optional.ofNullable(correlationIdProvider.get());
            context.ifPresent(MDC::setContextMap);
            korrelasjonsId.ifPresent(correlationIdSetter);
            try {
                in.run();
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                throw e;
            } finally {
                MDC.clear();
                contextBackup.ifPresent(MDC::setContextMap);
                backupKorrelasjonsId.ifPresent(correlationIdSetter);
            }
        };
    }
}
