select
	*
from
	(
	select
		bd.label bd_label,
		bd._id bd_id,
		de.community_type bd_community_type,
		round(de.confidence::numeric, 2) bd_confidence,
		round(
			( 100
				* (-- bg_area_percent
					(st_area(st_intersection(bg.geom, bd.geom), true))
					/ (st_area(bg.geom, true)))
				* (-- bd_area_percent
					(st_area(st_intersection(bg.geom, bd.geom), true))
					/ (st_area(bd.geom, true)))
				)::numeric,
			2
			) as bg_bd_area_confidence,
		--		round(
		--			st_area(
		--				st_intersection(
		--					bd.geom,
		--					st_intersection(bg.geom, bl.geom)
		--					),
		--				true)) bg_bl_bd_area,
		--		round(
		--			st_area(
		--				bl.geom,
		--				true)
		--			) bl_area,
		--		round((
		--			st_area(
		--				st_intersection(
		--					bd.geom, 
		--					st_intersection(bg.geom, bl.geom)
		--					),
		--				true)
		--			/ st_area(
		--				bl.geom,
		--				true))::numeric,
		--			2) bl_area_percent,
		--		bl.pop10 bl_pop10,
		--		round((
		--			st_area(st_intersection(bd.geom, st_intersection(bg.geom, bl.geom)),true) / st_area(bl.geom,true))
		--			* bl.pop10) bl_pop10_adjusted,
		--		bg.pop10,
		(round(st_area(st_intersection(bd.geom, st_intersection(bg.geom, bl.geom)), true) / st_area(bl.geom, true) * bl.pop10) 
			/ bg.pop10) bg_bl_pop_percent,
		sum(st_area(st_intersection(bg.geom, bd.geom), true))/ sum(st_area(bd.geom, true)) bd_area_percent
		--		sum(
		--			(st_area(st_intersection(bg.geom, bd.geom), true)/st_area(bg.geom, true))
		--			* (st_area(st_intersection(bg.geom, bd.geom), true)/st_area(bd.geom, true))
		--			) as bg_bl_area_confidence,
		--		sum(
		--			(st_area(st_intersection(bl.geom, bd.geom), true)/st_area(bd.geom, true))
		--			* ((st_area(st_intersection(bg.geom, st_intersection(bl.geom, bd.geom)), true)/st_area(bl.geom, true))*bl.pop10/bg.pop10)
		--			) as bg_pop_confidence,
		--		bd.geom as bd_geom
	from
		liveby.mongo_boundaries bd
	join (
		select 
			cs._id,
			cs.confidence,
			cs.community_type
		from
			temp.demographics_csv cs
		join liveby.mongo_boundaries mb 
			on
			cs._id = mb._id
		where
			cs.community_type in ('neighborhood', 'microNeighborhood', 'community', 'compositeCommunity')
		) de
		on
		de._id = bd._id
	left join tiger2019.bg_pophu bg
		on
		st_intersects(bd.geom,
		bg.geom)
		and not st_touches(bd.geom,
		bg.geom)
		and st_area(st_intersection(bd.geom,
		bg.geom),
		true) > 1
	left join tiger2010.tabblock bl
		on
		st_intersects(bd.geom,
		bl.geom)
		and not st_touches(bd.geom,
		bl.geom)
		and st_area(st_intersection(bd.geom,
		bl.geom),
		true) > 1
	where
		bl.pop10 > 0
		and bg.pop10 > 0
		and (st_area(st_intersection(bl.geom,
		bd.geom),
		true)/ st_area(bl.geom,
		true)) > .005
		and round((st_area(st_intersection(bl.geom, st_intersection(bd.geom, bg.geom)), true)/ st_area(bl.geom, true))::numeric, 2) > 0
	group by 
		bd.label,
		bd._id,
		bd.geom,
		de.community_type,
		de.confidence,
		bg.geom,
		bl.geom,
		bl.pop10,
		bg.pop10
	) parts
where
	bd_id in ('56699b21dd4700202391a9a5', '56699b21dd4700202391a9aa');
	
select 
	bd_label,
	bd_id,
	bd_community_type,
	bd_confidence,
	sum(bg_bd_area_confidence) bg_bd_area_confidence
from(
	select
		bd.label bd_label,
		bd._id bd_id,
		de.community_type bd_community_type,
		round(de.confidence::numeric,2) bd_confidence,
--		round(sum(st_area(st_intersection(bg.geom, bd.geom),true))) bg_bl_area,
--		round(sum(st_area(bg.geom,true))) bg_area,
--		sum(st_area(st_intersection(bg.geom, bd.geom), true))/sum(st_area(bg.geom, true)) bg_area_percent,
--		sum(st_area(bg.geom,true)) bd_area,
--		sum(st_area(st_intersection(bg.geom, bd.geom), true))/sum(st_area(bd.geom, true)) bd_area_percent,
		round((
			100
				* 	
					(
					(st_area(st_intersection(bg.geom, bd.geom), true))
					/ (st_area(bg.geom, true))
					) -- bg_area_percent
				* 	
					(
					(st_area(st_intersection(bg.geom, bd.geom), true))
					/ (st_area(bd.geom, true))
					) -- bd_area_percent
				)::numeric,
			2) as bg_bd_area_confidence
--		sum(
--			(st_area(st_intersection(bg.geom, bd.geom), true)/st_area(bg.geom, true))
--			* (st_area(st_intersection(bg.geom, bd.geom), true)/st_area(bd.geom, true))
--			) as bg_bl_area_confidence,
--		sum(
--			(st_area(st_intersection(bl.geom, bd.geom), true)/st_area(bd.geom, true))
--			* ((st_area(st_intersection(bg.geom, st_intersection(bl.geom, bd.geom)), true)/st_area(bl.geom, true))*bl.pop10/bg.pop10)
--			) as bg_pop_confidence,
--		bd.geom as bd_geom
	from
		liveby.mongo_boundaries bd
	join (
		select 
			cs._id,
			cs.confidence,
			cs.community_type
		from temp.demographics_csv cs
		join liveby.mongo_boundaries mb 
			on cs._id = mb._id
		where cs.community_type in ('neighborhood', 'microNeighborhood', 'community','compositeCommunity')
		) de
		on de._id = bd._id
	left join tiger2019.bg_pophu bg
		on st_intersects(bd.geom, bg.geom)
		and not st_touches(bd.geom, bg.geom)
		and st_area(st_intersection(bd.geom, bg.geom), true) > 1
	left join tiger2010.tabblock bl
		on st_intersects(bd.geom, bl.geom)
		and not st_touches(bd.geom, bl.geom)
		and st_area(st_intersection(bd.geom, bl.geom), true) > 1
	where
		bl.pop10 > 0
		and bg.pop10 > 0
		and (st_area(st_intersection(bl.geom, bd.geom), true)/st_area(bl.geom, true)) > .005
		and round((st_area(st_intersection(bl.geom, st_intersection(bd.geom, bg.geom)), true)/st_area(bl.geom, true))::numeric,2) > 0
	group by 
		bd.label,
		bd._id,
		bd.geom,
		de.community_type,
		de.confidence,
		bg.geom
	) parts
where bd_id in ('56699b21dd4700202391a9a5', '56699b21dd4700202391a9aa')
group by 	bd_label,
	bd_id,
	bd_community_type,
	bd_confidence;