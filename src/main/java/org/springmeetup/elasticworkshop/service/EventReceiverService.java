package org.springmeetup.elasticworkshop.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springmeetup.elasticworkshop.model.ListenEvent;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventReceiverService {

	private final EventProcessingService eventProcessingService;
	private final ObjectMapper objectMapper;

	@KafkaListener(topics = Constants.LISTEN_EVENT_TOPIC_NAME)
	public void listen(String message) {
		if (! StringUtils.hasText(message))
			return;

		log.info("Received Messasge from topic: {}, message: {} ", Constants.LISTEN_EVENT_TOPIC_NAME, message);

		try {
			ListenEvent listenEvent = objectMapper.readValue(message, ListenEvent.class);
			eventProcessingService.saveListenEvent(listenEvent);
		} catch (JsonProcessingException ex) {
			log.error("Json processing exception occured", ex);
		}
	}

}
