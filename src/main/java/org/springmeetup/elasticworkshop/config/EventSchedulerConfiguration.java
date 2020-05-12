package org.springmeetup.elasticworkshop.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springmeetup.elasticworkshop.service.ElasticSearchService;

import java.time.LocalDateTime;

@Configuration
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class EventSchedulerConfiguration {

	private final ElasticSearchService elasticSearchService;

	@Scheduled(cron = "${event.scheduler.cron}")
	public void runPartialIndexers() {
		log.info("updating rankings ... ");
		elasticSearchService.updateRankings();
	}
}
