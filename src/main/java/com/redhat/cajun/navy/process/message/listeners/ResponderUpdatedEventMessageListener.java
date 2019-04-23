package com.redhat.cajun.navy.process.message.listeners;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.redhat.cajun.navy.process.message.model.Message;
import com.redhat.cajun.navy.process.message.model.ResponderUpdatedEvent;
import org.jbpm.services.api.ProcessService;
import org.jbpm.services.api.query.QueryService;
import org.kie.api.runtime.process.ProcessInstance;
import org.kie.internal.KieInternalServices;
import org.kie.internal.process.CorrelationKey;
import org.kie.internal.process.CorrelationKeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class ResponderUpdatedEventMessageListener {

    private static final Logger log = LoggerFactory.getLogger(ResponderUpdatedEventMessageListener.class);

    private final static String TYPE_RESPONDER_UPDATED_EVENT = "ResponderUpdatedEvent";

    private static final String SIGNAL_RESPONDER_AVAILABLE = "ResponderAvailable";

    @Autowired
    private ProcessService processService;

    @Autowired
    private QueryService queryService;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private CorrelationKeyFactory correlationKeyFactory = KieInternalServices.Factory.get().newCorrelationKeyFactory();

    @KafkaListener(topics = "${listener.destination.responder-updated-event}")
    public void processMessage(@Payload String messageAsJson, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, Acknowledgment ack) {

        if (!accept(messageAsJson)) {
            ack.acknowledge();
            return;
        }

        log.debug("Processing '" + TYPE_RESPONDER_UPDATED_EVENT + "' message for responder '" + key + "' from topic:partition '" + topic + ":" + partition + "'");

        Message<ResponderUpdatedEvent> message;
        try {

            message = new ObjectMapper().readValue(messageAsJson, new TypeReference<Message<ResponderUpdatedEvent>>() {});

            String incidentId = message.getHeaderValue("incidentId");
            if (incidentId == null || incidentId.isEmpty()) {
                log.warn("Message contains no header value for incidentId. Message cannot be processed!");
                ack.acknowledge();
                return;
            }

            CorrelationKey correlationKey = correlationKeyFactory.newCorrelationKey(incidentId);

            Boolean available = "success".equals(message.getBody().getStatus());

            log.debug("Signaling process with correlationkey '" + correlationKey + ". Responder '" + key + "', available '" + available + "'." );

            final IntegerHolder holder = new IntegerHolder(1);
            while (holder.getValue() > 0 && holder.getValue() <= 3) {
                try {
                    TransactionTemplate template = new TransactionTemplate(transactionManager);
                    template.execute((TransactionStatus s) -> {
                        ProcessInstance instance = null;
                        // it seems that sometimes the process instance has not been updated in the database when calling getProcessInstance().
                        // dirty hack: if the process instance cannot be found, pause the thread for 300 ms and retry. Bail out after 3 times.
                        instance = processService.getProcessInstance(correlationKey);
                        if (instance == null) {
                            log.warn("Try " + holder.getValue() + " - Process instance with correlationKey '" + incidentId + "' not found.");
                            holder.increaseValue();
                            return null;
                        }
                        // check if process is waiting on 'ResponderAvailable' signal
                        if (!WaitingForSignalHelper.waitingForSignal(queryService, instance.getId(), "ResponderAvailable")) {
                            log.warn("Try " + holder.getValue() + " - Process instance with correlationKey '" + incidentId + "' is not waiting for signal 'ResponderAvailable'.");
                            holder.increaseValue();
                            return null;
                        }
                        log.info("Signaling process instance " + instance.getId() + " for incident '" + incidentId + "'");
                        processService.signalProcessInstance(instance.getId(), SIGNAL_RESPONDER_AVAILABLE, available);
                        holder.reset();
                        return null;
                    });
                } catch (UnexpectedRollbackException e) {
                    log.error("Transaction rolled back for incident '" + incidentId + "'", e);
                }
                if (holder.getValue() > 3) {
                    log.warn("Process instance with correlationKey '" + incidentId + "' is not waiting for signal 'ResponderAvailable'. Process instance is not signaled.");
                } else if (holder.getValue() > 0) {
                    log.warn("Sleeping for 300 ms");
                    Thread.sleep(300);
                }
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing msg " + messageAsJson, e);
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    private boolean accept(String messageAsJson) {
        try {
            String messageType = JsonPath.read(messageAsJson, "$.messageType");
            if (TYPE_RESPONDER_UPDATED_EVENT.equalsIgnoreCase(messageType) ) {
                return true;
            } else {
                log.debug("Message with type '" + messageType + "' is ignored");
            }
        } catch (Exception e) {
            log.warn("Unexpected message without 'messageType' field.");
        }
        return false;
    }

    public static class IntegerHolder {

        private int value;

        public IntegerHolder(int start) {
            value = start;
        }

        public void increaseValue() {
            value++;
        }

        public int getValue() {
            return value;
        }

        public void reset() {
            value = 0;
        }

    }
}
