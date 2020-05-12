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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;
import org.springmeetup.elasticworkshop.model.ArtistDocument;
import org.springmeetup.elasticworkshop.model.ArtistRanking;
import org.springmeetup.elasticworkshop.model.UserProfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticSearchService implements Constants {

	private final RestHighLevelClient client;
	private final ObjectMapper objectMapper;

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

	public BulkResponse executeBulkRequest(BulkRequest bulkRequest) {
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

	public String toJsonString(Object document) {
		String jsonString;
		try {
			jsonString = objectMapper.writeValueAsString(document);
		} catch (JsonProcessingException jpe) {
			throw new RuntimeException(jpe);
		}

		return jsonString;
	}

	public <T> T toDocumentObject(String jsonString, Class<T> clazz) {
		T result = null;
		try {
			result = objectMapper.readValue(jsonString, clazz);
		} catch (JsonProcessingException jpe) {
			throw new RuntimeException(jpe);
		}

		return result;

	}
}
