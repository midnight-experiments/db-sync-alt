-- The primary-key page precedes unit filtering: sparse matching units cannot
-- turn LIMIT into an unbounded scan, and dense blocks cannot enlarge a page.
-- All pages execute in the caller's one read-only repeatable-read snapshot.
WITH page AS MATERIALIZED (
 SELECT id,quantity,tx_out_id,ident FROM public.ma_tx_out
 WHERE id > $4::bigint AND id BETWEEN 0 AND $3::bigint*68719476736+68719476735
 ORDER BY id LIMIT 4096
), fixed AS (
 SELECT * FROM unnest($1::bytea[],$2::bytea[],$5::text[]) f(policy,name,previous)
)
SELECT encode(f.policy,'hex')||encode(f.name,'hex'),
 trunc(f.previous::numeric+coalesce(sum(a.quantity) FILTER (
  WHERE o.id IS NOT NULL AND NOT EXISTS (
   SELECT 1 FROM public.tx_in i
   WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index
    AND i.id BETWEEN 0 AND $3::bigint*16777216+16777215
  )
 ),0))::text,
 (SELECT max(id) FROM page), (SELECT count(*) FROM page)
FROM fixed f
LEFT JOIN public.multi_asset m ON m.policy=f.policy AND m.name=f.name
LEFT JOIN page a ON a.ident=m.id
LEFT JOIN public.tx_out o ON o.id=a.tx_out_id
 AND o.id BETWEEN 0 AND $3::bigint*16777216+16777215
GROUP BY f.policy,f.name,f.previous ORDER BY f.policy,f.name
