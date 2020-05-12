package org.springmeetup.elasticworkshop.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ArtistDocument {

	@JsonProperty("artist_id")
	private String artistId;

	@JsonProperty("artist_name")
	private String artistName;

	@JsonProperty("ranking")
	private Long ranking;

	@JsonProperty("_score")
	private Float _score;

}
