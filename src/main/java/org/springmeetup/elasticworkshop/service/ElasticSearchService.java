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
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springmeetup.elasticworkshop.model.*;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticSearchService {
	public final static String CONTENT_INDEX_NAME = "content";
	public final static String USER_PROFILE_INDEX_NAME = "user-profile";
	
	public final static String LISTEN_EVENT_INDEX_NAME_PREFIX = "listen-event-";
	public final static String ARTIST_RANKING_INDEX_NAME_PREFIX = "artist-ranking-";

	private final RestHighLevelClient client;
	private final ObjectMapper objectMapper;

	@Value("${listen-event.index.duration.inmins}")
	public int listenEventIndexDurationInMins;

	@Value("${artist-ranking.index.duration.inmins}")
	public int artistRankingIndexDurationInMins;

	/**
	 * performs following operations in elasticsearch
	 *  query string (full text)
	 *  ranking based boosting
	 *  user profile based boosting
	 *
	 * @param queryString
	 * @param from
	 * @param size
	 * @return
	 */
	public List<ArtistDocument> searchArtists(String queryString, String userId, int from, int size) {
		SearchRequest searchRequest = new SearchRequest(CONTENT_INDEX_NAME);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchRequest.source(searchSourceBuilder);

		UserProfile userProfile = getDocument(USER_PROFILE_INDEX_NAME, userId, UserProfile.class);
		
		// full text search - query string
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder()
				.should(new MultiMatchQueryBuilder(queryString)
							.field("artist_name", 2.0f) //artist_name token matches have double boost factor
							.field("artist_name.prefix", 1.0f)
							.type(MultiMatchQueryBuilder.Type.BEST_FIELDS)
							.operator(Operator.AND)
							.fuzziness("0")
				)
				.should(new MultiMatchQueryBuilder(queryString)
						.field("artist_name.prefix", 0.5f) //artist_name token matches have double boost factor
						.type(MultiMatchQueryBuilder.Type.BEST_FIELDS)
						.operator(Operator.AND)
						.fuzziness("1")
				)
				.minimumShouldMatch(1);

		List<ScriptScoreFunctionBuilder> scriptScoreFunctionBuilders = new ArrayList<>();

		// ranking based score function builder
		scriptScoreFunctionBuilders.add(ScoreFunctionBuilders.scriptFunction(
				"Math.max(_score * ((!doc['ranking'].empty ) ? Math.log(doc['ranking'].value) / Math.log(2) : 1)  - _score , 0)"
		));

		// user profile based score function builder
		if (userProfile != null && ! userProfile.getArtistRankingSet().isEmpty()) {
			String artistRankBooster = "";
			for (ArtistRanking artistRanking : userProfile.getArtistRankingSet()) {
				artistRankBooster += (artistRankBooster.isEmpty() ? "" : " * ") +
						String.format("((!doc['artist_id'].empty &&  doc['artist_id'].value == '%s') ? %f : 1)",
								artistRanking.getArtistId(),
								log2(artistRanking.getRanking()));
			}

			scriptScoreFunctionBuilders.add(ScoreFunctionBuilders.scriptFunction(
					"Math.max(_score * " + artistRankBooster + " - _score , 0)"
			));

		}
		
		FunctionScoreQueryBuilder.FilterFunctionBuilder[] filterFunctionBuilders = new FunctionScoreQueryBuilder.FilterFunctionBuilder[scriptScoreFunctionBuilders.size()];
		for (int i = 0; i < scriptScoreFunctionBuilders.size(); i++) {
			filterFunctionBuilders[i] = new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptScoreFunctionBuilders.get(i));
		}

		FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(boolQueryBuilder, filterFunctionBuilders)
				.scoreMode(FunctionScoreQuery.ScoreMode.SUM);

		searchSourceBuilder.query(functionScoreQueryBuilder);
		searchSourceBuilder.sort("_score", SortOrder.DESC);
		searchSourceBuilder.from(from);
		searchSourceBuilder.size(size);

		List<ArtistDocument> result = new ArrayList<>();
		try {
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			for (SearchHit searchHit : searchResponse.getHits().getHits()) {
				ArtistDocument artistDocument = toDocumentObject(searchHit.getSourceAsString(), ArtistDocument.class);
				artistDocument.set_score(searchHit.getScore());
				result.add(artistDocument);
			}
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}

		return result;
	}

	public static float log2(float x)
	{
		return (float) (Math.log(x) / Math.log(2));
	}
	

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
		AggregatedUserArtistRankings aggregatedUserArtistRankings = queryRecentAggregatedArtistRankingsFromListenEvents();
		Map<String, Long> artistRankingMap = aggregatedUserArtistRankings.getArtistRankingMap();
		Map<String, Set<ArtistRanking>> userArtistRankingMap = aggregatedUserArtistRankings.getUserArtistRankingMap();

		BulkRequest bulkUpdateRankingRequest = new BulkRequest();

		// update artist rankings
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

		// update user artist rankings
		for (String userId : userArtistRankingMap.keySet()) {
			Set<ArtistRanking> userArtistRankingSet = userArtistRankingMap.get(userId);

			UserProfile userProfile = getDocument(USER_PROFILE_INDEX_NAME, userId, UserProfile.class);

			// new user, index new document
			if (userProfile == null) {
				userProfile = UserProfile.builder()
						.userId(userId)
						.artistRankingSet(userArtistRankingSet)
						.build();

			} else {
				if (userProfile.getArtistRankingSet() == null) {
					userProfile.setArtistRankingSet(new HashSet<>());
				}

				// update existing user profile, this part should be definitely refactored
				for (ArtistRanking artistRanking : userArtistRankingSet) {
					ArtistRanking existingArtistRanking = userProfile.getArtistRankingSet().stream()
							.filter(artistRanking1 -> artistRanking1.getArtistId().equals(artistRanking.getArtistId()))
							.findFirst()
							.orElseGet(() -> null);

					if (existingArtistRanking == null) {
						userProfile.getArtistRankingSet().add(artistRanking);
					} else {
						long updatedRanking = existingArtistRanking.getRanking() == null ? 0 : existingArtistRanking.getRanking();
						updatedRanking += artistRanking.getRanking();

						existingArtistRanking.setRanking(updatedRanking);
					}
				}

			}

			IndexRequest userProfileIndexRequest = new IndexRequest(USER_PROFILE_INDEX_NAME);
			userProfileIndexRequest.id(userId);
			userProfileIndexRequest.source(toJsonString(userProfile), XContentType.JSON);
			bulkUpdateRankingRequest.add(userProfileIndexRequest);

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

	private AggregatedUserArtistRankings queryRecentAggregatedArtistRankingsFromListenEvents() {
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

		searchSourceBuilder.aggregation(AggregationBuilders.terms("users")
				.field("user_id")
				.size(1000)
				.subAggregation(AggregationBuilders.terms("artist_rankings")
						.field("artist_id")
						.size(1000)
				)
		);

		searchRequest.source(searchSourceBuilder);

		final Map<String, Long> artistRankingMap = new HashMap<>();
		final Map<String, Set<ArtistRanking>> userArtistRankingMap = new HashMap<>();
		final AggregatedUserArtistRankings aggregatedUserArtistRankings = AggregatedUserArtistRankings.builder()
				.artistRankingMap(artistRankingMap)
				.userArtistRankingMap(userArtistRankingMap)
				.build();

		try {
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

			if (searchResponse.getAggregations() != null) {
				Terms artistRankingsTerms = searchResponse.getAggregations().get("artist_rankings");

				artistRankingsTerms.getBuckets().stream()
						.map(bucket -> (Terms.Bucket) bucket) // cast to (Terms.Bucket)
						.forEach(termsBucket -> artistRankingMap.put(termsBucket.getKeyAsString(), termsBucket.getDocCount()))
				;

				Terms usersTerms = searchResponse.getAggregations().get("users");
				usersTerms.getBuckets().stream()
						.map(bucket -> (Terms.Bucket) bucket) // cast to (Terms.Bucket)
						.forEach(userBucket -> {
							String userId = userBucket.getKeyAsString();
							final Set<ArtistRanking> userArtistRankingSet = new HashSet<>();

							Terms userArtistRankingsTerms = userBucket.getAggregations().get("artist_rankings");
							userArtistRankingsTerms.getBuckets().stream()
									.map(artistRankingBucket -> (Terms.Bucket) artistRankingBucket)
									.forEach(artistRankingBucket -> userArtistRankingSet.add(ArtistRanking.builder()
											.artistId(artistRankingBucket.getKeyAsString())
											.ranking(artistRankingBucket.getDocCount())
											.build()
											)
									);

							userArtistRankingMap.put(userId, userArtistRankingSet);
						});
			}
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}

		return aggregatedUserArtistRankings;
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
