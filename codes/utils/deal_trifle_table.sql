DELETE FROM xmd_trifles_under_gov_control where source = 'trace_db' AND "intersection" = 'T' AND event_id || origin_id not in
(SELECT dd.event_id :: varchar || max(dd.origin_id) :: varchar from
(SELECT d.event_id, e.origin_id, d.search_cnt from
(SELECT c.event_id, max(c.search_cnt) as search_cnt
from
(SELECT max(b.event_id) as event_id, b.origin_id, max(b.search_cnt) as search_cnt
from (SELECT event_id, origin_id, search_cnt FROM xmd_trifles_under_gov_control where "source" = 'trace_db' AND "intersection" = 'T' ORDER BY (event_id, origin_id, search_cnt)) b inner join
  (
    SELECT id_route.event_id as event_id_chosen
      FROM
        (
          SELECT event_id, COUNT(DISTINCT(origin_id)) as route_num
            FROM xmd_trifles_under_gov_control WHERE "source" = 'trace_db' AND "intersection" = 'T' GROUP BY event_id
        ) id_route
        WHERE id_route.route_num > 1
  ) a
  on b.event_id = a.event_id_chosen group by b.origin_id) c group by c.event_id) d
  inner join
  (SELECT max(b.event_id) as event_id, b.origin_id, max(b.search_cnt) as search_cnt
from (SELECT event_id, origin_id, search_cnt FROM xmd_trifles_under_gov_control where "source" = 'trace_db' AND "intersection" = 'T' ORDER BY (event_id, origin_id, search_cnt)) b inner join
  (
    SELECT id_route.event_id as event_id_chosen
      FROM
        (
          SELECT event_id, COUNT(DISTINCT(origin_id)) as route_num
            FROM xmd_trifles_under_gov_control WHERE "source" = 'trace_db' AND "intersection" = 'T' GROUP BY event_id
        ) id_route
        WHERE id_route.route_num > 1
  ) a
  on b.event_id = a.event_id_chosen group by b.origin_id) e
  on (d.search_cnt = e.search_cnt and d.event_id = e.event_id) order by d.event_id) dd group by dd.event_id)  and event_id in
  (SELECT dd.event_id from
(SELECT d.event_id, e.origin_id, d.search_cnt from
(SELECT c.event_id, max(c.search_cnt) as search_cnt
from
(SELECT max(b.event_id) as event_id, b.origin_id, max(b.search_cnt) as search_cnt
from (SELECT event_id, origin_id, search_cnt FROM xmd_trifles_under_gov_control where "source" = 'trace_db' AND "intersection" = 'T' ORDER BY (event_id, origin_id, search_cnt)) b inner join
  (
    SELECT id_route.event_id as event_id_chosen
      FROM
        (
          SELECT event_id, COUNT(DISTINCT(origin_id)) as route_num
            FROM xmd_trifles_under_gov_control WHERE "source" = 'trace_db' AND "intersection" = 'T' GROUP BY event_id
        ) id_route
        WHERE id_route.route_num > 1
  ) a
  on b.event_id = a.event_id_chosen group by b.origin_id) c group by c.event_id) d
  inner join
  (SELECT max(b.event_id) as event_id, b.origin_id, max(b.search_cnt) as search_cnt
from (SELECT event_id, origin_id, search_cnt FROM xmd_trifles_under_gov_control where "source" = 'trace_db' AND "intersection" = 'T' ORDER BY (event_id, origin_id, search_cnt)) b inner join
  (
    SELECT id_route.event_id as event_id_chosen
      FROM
        (
          SELECT event_id, COUNT(DISTINCT(origin_id)) as route_num
            FROM xmd_trifles_under_gov_control WHERE "source" = 'trace_db' AND "intersection" = 'T' GROUP BY event_id
        ) id_route
        WHERE id_route.route_num > 1
  ) a
  on b.event_id = a.event_id_chosen group by b.origin_id) e
  on (d.search_cnt = e.search_cnt and d.event_id = e.event_id) order by d.event_id) dd group by dd.event_id);