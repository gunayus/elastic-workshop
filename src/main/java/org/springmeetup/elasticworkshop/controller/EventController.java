package org.springmeetup.elasticworkshop.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import org.springmeetup.elasticworkshop.model.ListenEvent;
import org.springmeetup.elasticworkshop.service.EventProcessingService;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/event")
@RequiredArgsConstructor
public class EventController {

	private final EventProcessingService eventProcessingService;

	@PostMapping("/listen-event")
	public void saveListenEvent(@RequestBody ListenEvent listenEvent,
	                            @RequestParam(value = "eventCount", defaultValue = "1") int eventCount) {
		if (listenEvent.getTimestamp() == null) {
			listenEvent.setTimestamp(LocalDateTime.now());
		}

		for (int i = 0; i < eventCount; i++) {
			eventProcessingService.saveListenEvent(listenEvent);
		}
	}

}
