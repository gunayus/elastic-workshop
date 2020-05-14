package org.springmeetup.elasticworkshop.controller;

import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.SearchResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springmeetup.elasticworkshop.model.ArtistDocument;
import org.springmeetup.elasticworkshop.service.ElasticSearchService;

import java.util.List;

@RestController
@RequestMapping("/search")
@RequiredArgsConstructor
public class SearchController {

	private final ElasticSearchService elasticSearchService;

	@GetMapping("/artist")
	public List<ArtistDocument> searchArtists(@RequestParam(name = "q", required = true) String queryString,
	                                          @RequestParam(name = "userid", required = true) String userId,
	                                          @RequestParam(name = "includeRanking", required = true) boolean includeRanking,
	                                          @RequestParam(name = "includeUserProfile", required = true) boolean includeUserProfile,
	                                          @RequestParam(name = "from", required = false, defaultValue = "0") Integer from,
	                                          @RequestParam(name = "size", required = false, defaultValue = "10") Integer size
	                                          ) {
		return elasticSearchService.searchArtists(queryString, userId, includeRanking, includeUserProfile, from, size);
	}
}
