-- The read behind the service's status: single-row tables, primary-key
-- lookups, the newest key of an indexed column, the call counts of the last
-- eight UTC days and tables bounded by the profile or the layout (unspent
-- counts per profile address, first observations per shape, upkeep records
-- per index). Nothing here grows with the served rows. Times are Unix
-- seconds; `identity` and `position` are the stored rows.
SELECT jsonb_build_object(
 'observed_at',floor(extract(epoch FROM clock_timestamp()))::bigint,
 'identity',(SELECT to_jsonb(i)-'id' FROM dsa.identity i WHERE id=1),
 'position',(SELECT to_jsonb(p)-'id' FROM dsa.position p WHERE id=1),
 'target',(SELECT jsonb_build_object('height',target_height,'hash',encode(target_hash,'hex'),
   'timestamp',floor(extract(epoch FROM target_time))::bigint) FROM dsa.position WHERE id=1 AND target_height IS NOT NULL),
 'tip',(SELECT jsonb_build_object('height',tip_height,'hash',encode(tip_hash,'hex'),
   'timestamp',floor(extract(epoch FROM tip_time))::bigint) FROM dsa.position WHERE id=1 AND tip_height IS NOT NULL),
 'horizon',(SELECT jsonb_build_object('height',horizon_height,'hash',encode(horizon_hash,'hex'),
   'timestamp',floor(extract(epoch FROM horizon_time))::bigint) FROM dsa.position WHERE id=1 AND horizon_height IS NOT NULL),
 'horizon_first_served_at',(SELECT floor(extract(epoch FROM f.first_served_at))::bigint
   FROM dsa.position p JOIN dsa.first_served f ON f.height=p.horizon_height WHERE p.id=1),
 'current_epoch',(SELECT b.epoch_no FROM dsa.position p JOIN serve.block b ON b.id=p.tip_height WHERE p.id=1),
 'newest_parameters_epoch',(SELECT max(epoch_no) FROM serve.epoch_param),
 'newest_stake_epoch',(SELECT max(epoch_no) FROM serve.epoch_stake),
 'service_state',(SELECT jsonb_build_object('state',state,'reason',reason,'detail',detail,
   'last_update',last_update,'last_error',last_error,'updated_at',floor(extract(epoch FROM updated_at))::bigint,
   'last_update_at',floor(extract(epoch FROM (last_update->>'at')::timestamptz))::bigint,
   'last_error_at',floor(extract(epoch FROM (last_error->>'at')::timestamptz))::bigint)
   FROM dsa.service_state WHERE id=1),
 'calls',(SELECT coalesce(jsonb_agg(jsonb_build_object('day',day,'requests',requests) ORDER BY day),'[]'::jsonb)
   FROM dsa.calls WHERE day>=(now() AT TIME ZONE 'UTC')::date-7),
 'unspent',(SELECT coalesce(jsonb_object_agg(address,outputs),'{}'::jsonb) FROM dsa.unspent),
 'first_observations',(SELECT coalesce(jsonb_agg(jsonb_build_object('shape',shape,'block_height',block_height,
   'transaction',encode(transaction_hash,'hex'),'output_index',output_index,'datum_hash',encode(datum_hash,'hex'),
   'raw_datum_bytes',raw_datum_bytes,'observed_at',floor(extract(epoch FROM observed_at))::bigint,
   'answers',(SELECT coalesce(jsonb_agg(jsonb_build_object('source',u.source,'bytes',octet_length(u.answer),
     'sha256',encode(sha256(u.answer),'hex')) ORDER BY u.n),'[]'::jsonb)
     FROM unnest(o.sources,o.answers) WITH ORDINALITY u(source,answer,n))) ORDER BY shape),'[]'::jsonb)
   FROM dsa.first_observation o),
 'upkeep',(SELECT coalesce(jsonb_object_agg(name,jsonb_build_object('at',floor(extract(epoch FROM at))::bigint,
   'detail',detail)),'{}'::jsonb) FROM dsa.upkeep),
 'size_bytes',pg_database_size(current_database()),
 'tls_available',current_setting('ssl')='on')
