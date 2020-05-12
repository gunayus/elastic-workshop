package org.springmeetup.elasticworkshop.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode
public class ArtistRanking {

	@JsonProperty("artist_id")
	private String artistId;

	@JsonProperty("ranking")
	@EqualsAndHashCode.Exclude
	private Long ranking;

}
