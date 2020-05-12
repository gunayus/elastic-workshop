package org.springmeetup.elasticworkshop.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticSearchService {
	private final String CONTENT_INDEX_NAME = "content";
	private final String LISTEN_EVENT_INDEX_NAME_PREFIX = "listen-event-";

	private final RestHighLevelClient client;
	private final ObjectMapper objectMapper;

	@Value("${event.index.duration.inmins}")
	private int indexDurationInMins;

	/**
	 * return the index name of the current period for indexing event documents
	 * e.g. if current time is 2020-05-14 18:30 and indexDurationInMins is 5, index name will be
	 * listen-event-2020-05-14-18:30
	 *
	 * @return
	 */
	public String getCurrentIndexName() {
		return getIndexName(LocalDateTime.now());
	}

	/**
	 * return the index name of the previous period for querying aggregated event documents
	 * e.g. if current time is 2020-05-14 18:30 and indexDurationInMins is 5, index name will be
	 * listen-event-2020-05-14-18:25
	 *
	 * @return
	 */
	public String getPreviousIndexName() {
		return getIndexName(LocalDateTime.now().minus(Duration.ofMinutes(indexDurationInMins)));
	}

	private String getIndexName(LocalDateTime timestamp) {
		int currentMinute = timestamp.getMinute();
		String minutePart = String.format("%02d", ((currentMinute / indexDurationInMins) * indexDurationInMins));

		String indexName = LISTEN_EVENT_INDEX_NAME_PREFIX + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")) + "-" + minutePart;
		return indexName;
	}

	public void updateRankings() {
		Map<String, Long> artistRankingMap = queryRecentAggregatedArtistRankings();

		BulkRequest bulkUpdateRankingRequest = new BulkRequest();

		for (String artistId : artistRankingMap.keySet()) {
			UpdateRequest updateArtistRankingRequest = new UpdateRequest(CONTENT_INDEX_NAME, artistId);

			Map<String, Object> parameters = Collections.singletonMap("count", artistRankingMap.get(artistId));

			Script inline = new Script(ScriptType.INLINE, "painless",
					"if (ctx._source.ranking == null) { ctx._source.ranking = params.count } else { ctx._source.ranking += params.count }", parameters);
			updateArtistRankingRequest.script(inline);

			bulkUpdateRankingRequest.add(updateArtistRankingRequest);
		}

		if (bulkUpdateRankingRequest.numberOfActions() > 0) {
			executeBulkRequest(bulkUpdateRankingRequest);
		}
	}

	private Map<String, Long> queryRecentAggregatedArtistRankings() {
		String indexName = getPreviousIndexName();
		log.info("querying recent artist rankings from index [{}]", indexName);

		SearchRequest searchRequest = new SearchRequest(indexName);
		searchRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, true));

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		searchSourceBuilder.query(queryBuilder);
		searchSourceBuilder.aggregation(AggregationBuilders.terms("artist_rankings")
				.field("artist_id")
				.size(1000)
		);

		searchRequest.source(searchSourceBuilder);

		final Map<String, Long> artistRankingMap = new HashMap<>();
		try {
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

			if (searchResponse.getAggregations() != null) {
				Terms terms = searchResponse.getAggregations().get("artist_rankings");

				terms.getBuckets().stream()
						.map(bucket -> (Terms.Bucket) bucket) // cast to (Terms.Bucket)
						.forEach(termsBucket -> artistRankingMap.put(termsBucket.getKeyAsString(), termsBucket.getDocCount()))
				;
			}
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}

		return artistRankingMap;
	}

	/**
	 * indexes a given document in the given index
	 * if id is null, elasticsearch generates a new unique document id, otherwise given id is used as the document id
	 *
	 * @param indexName
	 * @param id
	 * @param document
	 * @return
	 */
	public IndexResponse indexDocument(String indexName, String id, Object document) {
		IndexRequest indexRequest = new IndexRequest(indexName);

		if (id != null) {
			indexRequest.id(id);
		}

		String jsonString;
		try {
			jsonString = objectMapper.writeValueAsString(document);
		} catch (JsonProcessingException jpe) {
			throw new RuntimeException(jpe);
		}

		indexRequest.source(jsonString, XContentType.JSON);

		IndexResponse indexResponse;
		try {
			indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}

		return indexResponse;
	}

	private BulkResponse executeBulkRequest(BulkRequest bulkRequest) {
		BulkResponse response = null;
		try {
			response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
			log.info(" executed " + bulkRequest.numberOfActions() + " bulk documents...");

			for (BulkItemResponse bulkItemResponse : response.getItems()) {
				if (response.hasFailures()) {
					log.error("\t bulk request failure : {}", bulkItemResponse.getFailureMessage());
				}
			}
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}

		return response;
	}

}
