package org.springmeetup.elasticworkshop.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AggregatedUserArtistRankings {

	private Map<String, Long> artistRankingMap;

	private Map<String, Set<ArtistRanking>> userArtistRankingMap;

}
