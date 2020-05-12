package org.springmeetup.elasticworkshop.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ArtistRanking {

	@JsonProperty("artist_id")
	private String artistId;

	@JsonProperty("ranking")
	private Long ranking;

}
