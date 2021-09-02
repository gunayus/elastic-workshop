package org.springmeetup.elasticworkshop.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springmeetup.elasticworkshop.model.ListenEvent;
import org.springmeetup.elasticworkshop.service.Constants;
import org.springmeetup.elasticworkshop.service.EventProcessingService;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/event")
@RequiredArgsConstructor
public class EventController {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;

	@PostMapping("/listen-event")
	public void saveListenEvent(@RequestBody ListenEvent listenEvent,
	                            @RequestParam(value = "eventCount", defaultValue = "1") int eventCount) throws JsonProcessingException {
		if (listenEvent.getTimestamp() == null) {
			listenEvent.setTimestamp(LocalDateTime.now());
		}

		for (int i = 0; i < eventCount; i++) {
			kafkaTemplate.send(Constants.LISTEN_EVENT_TOPIC_NAME, objectMapper.writeValueAsString(listenEvent));
		}
	}

}
