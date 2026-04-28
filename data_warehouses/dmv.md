
```sql
SELECT*FROM sys.dm_exec_connections

select* from sys.dm_exec_sessions

select* from sys.dm_exec_requests
```
-- joined views Modify the SQL code to join the DMVs and return information about currently running requests in the same database


```sql
SELECT connections.connection_id,
sessions.session_id, sessions.login_name, sessions.login_time,
requests.command, requests.start_time, requests.total_elapsed_time
FROM sys.dm_exec_connections AS connections
INNER JOIN sys.dm_exec_sessions AS sessions
   ON connections.session_id=sessions.session_id
INNER JOIN sys.dm_exec_requests AS requests
   ON requests.session_id = sessions.session_id
WHERE requests.status = 'running'
   AND requests.database_id = DB_ID()
ORDER BY requests.total_elapsed_time DESC;
```
