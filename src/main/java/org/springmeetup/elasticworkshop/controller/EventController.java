package org.springmeetup.elasticworkshop.controller;

import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.index.IndexResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springmeetup.elasticworkshop.model.ListenEvent;
import org.springmeetup.elasticworkshop.service.ElasticSearchService;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@RestController
@RequestMapping("/event")
@RequiredArgsConstructor
public class EventController {

	private final ElasticSearchService elasticSearchService;

	@PostMapping("/listen-event")
	public IndexResponse saveListenEvent(@RequestBody ListenEvent listenEvent) {
		if (listenEvent.getTimestamp() == null) {
			listenEvent.setTimestamp(LocalDateTime.now());
		}

		String indexName = elasticSearchService.getCurrentIndexName();
		return elasticSearchService.indexDocument(indexName, null, listenEvent);
	}

}
