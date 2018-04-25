select owner, table_name, tablespace_name, status, num_rows, blocks, to_char(last_analyzed, 'mm/dd/yyyy hh24:mi:ss') as last_analyzed
from all_tables
where
    table_name not like '%LALA%'
    and table_name not like '%LOLO%'
    --and owner in  ('SYS', 'SYSTEM')
    and tablespace_name in ('SYSAUX', 'SYSTEM')
 order by 1,2
 
 
 ----------- DRAFT ----------
 select distinct last_analyzed 
 from (
     select owner, table_name, tablespace_name, status, num_rows, blocks, to_char(last_analyzed, 'mm/dd/yyyy hh24:mi:ss') as last_analyzed
    from all_tables
    where
        table_name not like '%LALA%'
        and table_name not like '%LOLO%'
        --and owner in  ('SYS', 'SYSTEM')
        and tablespace_name in ('SYSAUX', 'SYSTEM')
)        
    