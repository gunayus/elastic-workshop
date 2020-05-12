package org.springmeetup.elasticworkshop.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

@Configuration
public class ElasticSearchConfiguration {

	@Value("#{'${elasticsearch.hosts}'.split(',')}")
	private List<String> elasticHosts;

	@Bean("elasticsearchObjectMapper")
	public ObjectMapper objectMapper() {
		return new ObjectMapper()
				.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
				.registerModule(new JavaTimeModule())
				;
	}

	@Bean
	public RestHighLevelClient client() {
		// parse elastic hosts config property, map tp HttpHost's array
		HttpHost[] httpHostArray = elasticHosts.stream()
				.map(this::createUrl)
				.map(u -> new HttpHost(u.getHost(), u.getPort(), u.getProtocol()))
				.toArray(HttpHost[]::new);

		SniffOnFailureListener sniffOnFailureListener = new SniffOnFailureListener();

		// low level rest client
		RestClientBuilder restClientBuilder = RestClient.builder(httpHostArray)
				.setFailureListener(sniffOnFailureListener);

		// high level rest client
		RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

		// create sniffer and set to failure listener
		Sniffer sniffer = Sniffer.builder(restHighLevelClient.getLowLevelClient())
				.setSniffAfterFailureDelayMillis(30000)
				.build();
		sniffOnFailureListener.setSniffer(sniffer);

		return restHighLevelClient;
	}

	private URL createUrl(String url) {
		try {
			return new URL(url);
		} catch (MalformedURLException error) {
			throw new IllegalArgumentException(error.getMessage(), error);
		}
	}
}