package org.springmeetup.elasticworkshop.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ListenEvent {

	@JsonProperty("artist_id")
	private String artistId;

	@JsonProperty("song_id")
	private String songId;

	@JsonProperty("user_id")
	private String userId;

	@JsonProperty( value = "timestamp")
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern ="yyyy-MM-dd'T'HH:mm:ss.SSS")
	private LocalDateTime timestamp;

}
