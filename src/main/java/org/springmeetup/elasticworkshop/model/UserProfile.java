package org.springmeetup.elasticworkshop.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserProfile {

	@JsonProperty("user_id")
	private String userId;

	@JsonProperty("artist_ranking")
	private Set<ArtistRanking> artistRankingSet;

}
