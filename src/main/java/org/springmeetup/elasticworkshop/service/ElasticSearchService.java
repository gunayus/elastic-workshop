package org.springmeetup.elasticworkshop.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springmeetup.elasticworkshop.model.ArtistRanking;
import org.springmeetup.elasticworkshop.model.ListenEvent;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticSearchService {
	public final static String CONTENT_INDEX_NAME = "content";
	public final static String LISTEN_EVENT_INDEX_NAME_PREFIX = "listen-event-";
	public final static String ARTIST_RANKING_INDEX_NAME_PREFIX = "artist-ranking-";

	private final RestHighLevelClient client;
	private final ObjectMapper objectMapper;

	@Value("${listen-event.index.duration.inmins}")
	public int listenEventIndexDurationInMins;

	@Value("${artist-ranking.index.duration.inmins}")
	public int artistRankingIndexDurationInMins;

	/**
	 * return the index name of the current period for indexing event documents
	 * e.g. if current time is 2020-05-14 18:30 and indexDurationInMins is 5, index name will be
	 * listen-event-2020-05-14-18:30
	 *
	 * @return
	 */
	public String getCurrentIndexName(String indexPrefix, int indexDurationInMins) {
		return getIndexName(indexPrefix, indexDurationInMins, LocalDateTime.now());
	}

	/**
	 * return the index name of the previous period for querying aggregated event documents
	 * e.g. if current time is 2020-05-14 18:30 and indexDurationInMins is 5, index name will be
	 * listen-event-2020-05-14-18:25
	 *
	 * @return
	 */
	public String getPreviousIndexName(String indexPrefix, int indexDurationInMins) {
		return getIndexName(indexPrefix, indexDurationInMins, LocalDateTime.now().minus(Duration.ofMinutes(indexDurationInMins)));
	}

	private String getIndexName(String indexPrefix, int indexDurationInMins, LocalDateTime timestamp) {
		long instantSeconds = timestamp.atZone(ZoneId.systemDefault()).toEpochSecond();
		long instantMinutes = instantSeconds / 60;
		long indexMinutes = (instantMinutes / indexDurationInMins) * indexDurationInMins;

		timestamp = timestamp.minusMinutes(instantMinutes - indexMinutes);

		String indexName = indexPrefix + timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm"));
		return indexName;
	}

	public void updateArtistRankings() {
		Map<String, Long> artistRankingMap = queryRecentAggregatedArtistRankingsFromListenEvents();

		BulkRequest bulkUpdateRankingRequest = new BulkRequest();

		for (String artistId : artistRankingMap.keySet()) {
			Map<String, Object> parameters = Collections.singletonMap("count", artistRankingMap.get(artistId));
			Script inline = new Script(ScriptType.INLINE, "painless",
					"if (ctx._source.ranking == null) { ctx._source.ranking = params.count } else { ctx._source.ranking += params.count }", parameters);

			// update artist ranking in content index
			UpdateRequest updateArtistRankingRequest = new UpdateRequest(CONTENT_INDEX_NAME, artistId);
			updateArtistRankingRequest.script(inline);
			bulkUpdateRankingRequest.add(updateArtistRankingRequest);

			// upsert ArtistRanking document in current daily historical artist_rankings index
			String currentDailyArtistRankingIndexName = getCurrentIndexName(ARTIST_RANKING_INDEX_NAME_PREFIX, artistRankingIndexDurationInMins);
			ArtistRanking artistRanking = getDocument(currentDailyArtistRankingIndexName, artistId, ArtistRanking.class);
			if (artistRanking == null) {
				artistRanking = ArtistRanking.builder()
						.artistId(artistId)
						.ranking(artistRankingMap.get(artistId))
						.build();

				IndexRequest artistRankingIndexRequest = new IndexRequest(currentDailyArtistRankingIndexName);
				artistRankingIndexRequest.id(artistId);
				artistRankingIndexRequest.source(toJsonString(artistRanking), XContentType.JSON);
				bulkUpdateRankingRequest.add(artistRankingIndexRequest);
			} else {
				UpdateRequest updateDailyArtistRankingRequest = new UpdateRequest(currentDailyArtistRankingIndexName, artistId);
				updateDailyArtistRankingRequest.script(inline);
				bulkUpdateRankingRequest.add(updateDailyArtistRankingRequest);
			}
		}

		if (bulkUpdateRankingRequest.numberOfActions() > 0) {
			executeBulkRequest(bulkUpdateRankingRequest);
		}
	}

	/**
	 * indexes a new listenEvent document in current event index
	 * @param listenEvent
	 * @return
	 */
	public IndexResponse saveListenEvent(ListenEvent listenEvent) {
		String indexName = getCurrentIndexName(LISTEN_EVENT_INDEX_NAME_PREFIX, listenEventIndexDurationInMins);
		return indexDocument(indexName, null, listenEvent);
	}

	private Map<String, Long> queryRecentAggregatedArtistRankingsFromListenEvents() {
		String indexName = getPreviousIndexName(LISTEN_EVENT_INDEX_NAME_PREFIX, listenEventIndexDurationInMins);
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
		indexRequest.source(toJsonString(document), XContentType.JSON);

		IndexResponse indexResponse;
		try {
			indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}

		return indexResponse;
	}

	public <T> T getDocument(String indexName, String id, Class<T> clazz) {
		GetRequest getRequest = new GetRequest(indexName, id);
		GetResponse getResponse = null;

		try {
			getResponse = client.get(getRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ElasticsearchStatusException esse) {
			if (esse.status().equals(RestStatus.NOT_FOUND)) {
				// ignore index not found : 404, this might be the first document to be indexed in the periodic index
				return null;
			}

			throw esse;
		}

		T result = null;
		if (getResponse.isExists()) {
			result = toDocumentObject(getResponse.getSourceAsString(), clazz);
		}

		return result;
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

	private String toJsonString(Object document) {
		String jsonString;
		try {
			jsonString = objectMapper.writeValueAsString(document);
		} catch (JsonProcessingException jpe) {
			throw new RuntimeException(jpe);
		}

		return jsonString;
	}

	private <T> T toDocumentObject(String jsonString, Class<T> clazz) {
		T result = null;
		try {
			result = objectMapper.readValue(jsonString, clazz);
		} catch (JsonProcessingException jpe) {
			throw new RuntimeException(jpe);
		}

		return result;

	}
}
