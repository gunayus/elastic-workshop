package org.springmeetup.elasticworkshop.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
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
import org.springmeetup.elasticworkshop.model.AggregatedUserArtistRankings;
import org.springmeetup.elasticworkshop.model.ArtistRanking;
import org.springmeetup.elasticworkshop.model.ListenEvent;
import org.springmeetup.elasticworkshop.model.UserProfile;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventProcessingService implements Constants {

	private final RestHighLevelClient client;
	private final ElasticSearchService elasticSearchService;

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
			ArtistRanking artistRanking = elasticSearchService.getDocument(currentDailyArtistRankingIndexName, artistId, ArtistRanking.class);
			if (artistRanking == null) {
				artistRanking = ArtistRanking.builder()
						.artistId(artistId)
						.ranking(artistRankingMap.get(artistId))
						.build();

				IndexRequest artistRankingIndexRequest = new IndexRequest(currentDailyArtistRankingIndexName);
				artistRankingIndexRequest.id(artistId);
				artistRankingIndexRequest.source(elasticSearchService.toJsonString(artistRanking), XContentType.JSON);
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

			UserProfile userProfile = elasticSearchService.getDocument(USER_PROFILE_INDEX_NAME, userId, UserProfile.class);

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
			userProfileIndexRequest.source(elasticSearchService.toJsonString(userProfile), XContentType.JSON);
			bulkUpdateRankingRequest.add(userProfileIndexRequest);

		}
		if (bulkUpdateRankingRequest.numberOfActions() > 0) {
			elasticSearchService.executeBulkRequest(bulkUpdateRankingRequest);
		}
	}

	/**
	 * indexes a new listenEvent document in current event index
	 * @param listenEvent
	 * @return
	 */
	public IndexResponse saveListenEvent(ListenEvent listenEvent) {
		String indexName = getCurrentIndexName(LISTEN_EVENT_INDEX_NAME_PREFIX, listenEventIndexDurationInMins);
		return elasticSearchService.indexDocument(indexName, null, listenEvent);
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


}
