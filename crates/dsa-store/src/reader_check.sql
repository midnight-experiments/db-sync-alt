-- Effective privileges include grants inherited through PUBLIC or column ACLs.
SELECT reason FROM (
 SELECT 'role_membership' AS reason WHERE EXISTS (
  SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname=$1
 )
 UNION ALL
 SELECT 'database_'||lower(privilege) FROM unnest(ARRAY['CREATE','TEMPORARY']) privilege
 WHERE has_database_privilege($1,current_database(),privilege)
 UNION ALL
 SELECT 'schema:'||n.nspname FROM pg_namespace n
 WHERE n.nspname NOT LIKE 'pg\_%' AND n.nspname<>'information_schema'
 AND (has_schema_privilege($1,n.oid,'CREATE') OR
      (n.nspname<>'public' AND has_schema_privilege($1,n.oid,'USAGE')))
 UNION ALL
 SELECT 'relation:'||n.nspname||'.'||c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname NOT LIKE 'pg\_%' AND n.nspname<>'information_schema'
 AND c.relkind IN ('r','p','v','m','f','S') AND (
  CASE WHEN c.relkind='S' THEN has_sequence_privilege($1,c.oid,'USAGE,SELECT,UPDATE') ELSE
   has_table_privilege($1,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') OR
   has_any_column_privilege($1,c.oid,'INSERT,UPDATE,REFERENCES') OR
   has_table_privilege($1,c.oid,'SELECT WITH GRANT OPTION') OR
   ((has_table_privilege($1,c.oid,'SELECT') OR has_any_column_privilege($1,c.oid,'SELECT')) AND
    NOT (n.nspname='public' AND c.relname=ANY($2::text[])))
  END)
 UNION ALL
 SELECT 'missing:'||name FROM unnest($2::text[]) name
 WHERE NOT has_table_privilege($1,format('public.%I',name),'SELECT')
 UNION ALL
 SELECT 'execute:'||p.oid::regprocedure::text FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname NOT LIKE 'pg\_%' AND n.nspname<>'information_schema'
 AND p.oid<>'dsa.gate()'::regprocedure AND has_function_privilege($1,p.oid,'EXECUTE')
 UNION ALL
 SELECT 'missing:gate' WHERE NOT has_function_privilege($1,'dsa.gate()','EXECUTE')
 UNION ALL
 SELECT 'missing:public_usage' WHERE NOT has_schema_privilege($1,'public','USAGE')
 UNION ALL
 SELECT 'missing:connect' WHERE NOT has_database_privilege($1,current_database(),'CONNECT')
) violations ORDER BY reason
