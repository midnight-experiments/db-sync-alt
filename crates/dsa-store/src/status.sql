-- The store-level report of `status`.
SELECT jsonb_build_object(
 'identity',(SELECT to_jsonb(i)-'id' FROM dsa.identity i WHERE id=1),
 'position',(SELECT to_jsonb(p)-'id' FROM dsa.position p WHERE id=1),
 'service_state',(SELECT to_jsonb(s)-'id' FROM dsa.service_state s WHERE id=1),
 'unspent',(SELECT coalesce(jsonb_object_agg(address,outputs),'{}'::jsonb) FROM dsa.unspent),
 'first_observations',(SELECT coalesce(jsonb_agg(jsonb_build_object('shape',shape,'block_height',block_height,
   'transaction',encode(transaction_hash,'hex'),'output_index',output_index,'datum_hash',encode(datum_hash,'hex'),
   'raw_datum_bytes',raw_datum_bytes,'sources',sources,
   'answer_bytes',(SELECT jsonb_agg(octet_length(a)) FROM unnest(answers) a),'observed_at',observed_at) ORDER BY shape),'[]'::jsonb)
   FROM dsa.first_observation),
 'calls',(SELECT coalesce(jsonb_object_agg(day,requests ORDER BY day),'{}'::jsonb) FROM dsa.calls
   WHERE day>=(now() AT TIME ZONE 'UTC')::date-7))
