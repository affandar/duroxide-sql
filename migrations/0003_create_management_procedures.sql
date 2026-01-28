-- Migration 0003: Create management stored procedures
-- ProviderAdmin support procedures

-- ============================================================================
-- List Instances
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_list_instances', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_list_instances;
GO

CREATE PROCEDURE [{SCHEMA}].sp_list_instances
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT instance_id
    FROM [{SCHEMA}].instances
    ORDER BY created_at DESC;
END;
GO

-- ============================================================================
-- List Instances by Status
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_list_instances_by_status', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_list_instances_by_status;
GO

CREATE PROCEDURE [{SCHEMA}].sp_list_instances_by_status
    @status NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT i.instance_id
    FROM [{SCHEMA}].instances i
    JOIN [{SCHEMA}].executions e ON i.instance_id = e.instance_id 
        AND i.current_execution_id = e.execution_id
    WHERE e.status = @status
    ORDER BY i.created_at DESC;
END;
GO

-- ============================================================================
-- List Executions
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_list_executions', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_list_executions;
GO

CREATE PROCEDURE [{SCHEMA}].sp_list_executions
    @instance_id NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT execution_id
    FROM [{SCHEMA}].executions
    WHERE instance_id = @instance_id
    ORDER BY execution_id;
END;
GO

-- ============================================================================
-- Get Instance Info
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_get_instance_info', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_get_instance_info;
GO

CREATE PROCEDURE [{SCHEMA}].sp_get_instance_info
    @instance_id NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        i.instance_id,
        i.orchestration_name,
        COALESCE(i.orchestration_version, 'unknown') AS orchestration_version,
        i.current_execution_id,
        e.status,
        e.output,
        CONVERT(NVARCHAR(30), i.created_at, 127) AS created_at,
        i.parent_instance_id
    FROM [{SCHEMA}].instances i
    LEFT JOIN [{SCHEMA}].executions e ON i.instance_id = e.instance_id 
        AND i.current_execution_id = e.execution_id
    WHERE i.instance_id = @instance_id;
END;
GO

-- ============================================================================
-- Get Execution Info
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_get_execution_info', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_get_execution_info;
GO

CREATE PROCEDURE [{SCHEMA}].sp_get_execution_info
    @instance_id NVARCHAR(255),
    @execution_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        e.execution_id,
        e.status,
        e.output,
        CONVERT(NVARCHAR(30), e.started_at, 127) AS started_at,
        CONVERT(NVARCHAR(30), e.completed_at, 127) AS completed_at,
        COALESCE((SELECT COUNT(*) FROM [{SCHEMA}].history h 
                  WHERE h.instance_id = @instance_id AND h.execution_id = @execution_id), 0) AS event_count
    FROM [{SCHEMA}].executions e
    WHERE e.instance_id = @instance_id AND e.execution_id = @execution_id;
END;
GO

-- ============================================================================
-- Get System Metrics
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_get_system_metrics', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_get_system_metrics;
GO

CREATE PROCEDURE [{SCHEMA}].sp_get_system_metrics
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        (SELECT COUNT(*) FROM [{SCHEMA}].instances) AS total_instances,
        (SELECT COUNT(*) FROM [{SCHEMA}].executions) AS total_executions,
        (SELECT COUNT(DISTINCT i.instance_id) 
         FROM [{SCHEMA}].instances i
         JOIN [{SCHEMA}].executions e ON i.instance_id = e.instance_id 
            AND i.current_execution_id = e.execution_id
         WHERE e.status = 'Running') AS running_instances,
        (SELECT COUNT(DISTINCT i.instance_id) 
         FROM [{SCHEMA}].instances i
         JOIN [{SCHEMA}].executions e ON i.instance_id = e.instance_id 
            AND i.current_execution_id = e.execution_id
         WHERE e.status = 'Completed') AS completed_instances,
        (SELECT COUNT(DISTINCT i.instance_id) 
         FROM [{SCHEMA}].instances i
         JOIN [{SCHEMA}].executions e ON i.instance_id = e.instance_id 
            AND i.current_execution_id = e.execution_id
         WHERE e.status = 'Failed') AS failed_instances,
        (SELECT COUNT(*) FROM [{SCHEMA}].history) AS total_events;
END;
GO

-- ============================================================================
-- Get Queue Depths
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_get_queue_depths', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_get_queue_depths;
GO

CREATE PROCEDURE [{SCHEMA}].sp_get_queue_depths
    @now_ms BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        (SELECT COUNT(*) FROM [{SCHEMA}].orchestrator_queue 
         WHERE visible_at <= SYSUTCDATETIME()
           AND (lock_token IS NULL OR locked_until <= @now_ms)) AS orchestrator_queue,
        (SELECT COUNT(*) FROM [{SCHEMA}].worker_queue 
         WHERE visible_at <= @now_ms
           AND (lock_token IS NULL OR locked_until <= @now_ms)) AS worker_queue;
END;
GO

-- ============================================================================
-- List Children
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_list_children', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_list_children;
GO

CREATE PROCEDURE [{SCHEMA}].sp_list_children
    @instance_id NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT instance_id AS child_instance_id
    FROM [{SCHEMA}].instances
    WHERE parent_instance_id = @instance_id
    ORDER BY created_at;
END;
GO

-- ============================================================================
-- Get Parent ID
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_get_parent_id', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_get_parent_id;
GO

CREATE PROCEDURE [{SCHEMA}].sp_get_parent_id
    @instance_id NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT parent_instance_id
    FROM [{SCHEMA}].instances
    WHERE instance_id = @instance_id;
END;
GO

-- ============================================================================
-- Delete Instances Atomic
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_delete_instances_atomic', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_delete_instances_atomic;
GO

CREATE PROCEDURE [{SCHEMA}].sp_delete_instances_atomic
    @instance_ids NVARCHAR(MAX),  -- JSON array
    @force BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @instances_deleted BIGINT = 0;
    DECLARE @executions_deleted BIGINT = 0;
    DECLARE @events_deleted BIGINT = 0;
    DECLARE @queue_deleted BIGINT = 0;
    
    BEGIN TRANSACTION;
    BEGIN TRY
        -- Create temp table of instance IDs
        CREATE TABLE #ids (instance_id NVARCHAR(255));
        INSERT INTO #ids SELECT value FROM OPENJSON(@instance_ids);
        
        -- Check for running instances if not force
        IF @force = 0
        BEGIN
            IF EXISTS (
                SELECT 1 FROM [{SCHEMA}].instances i
                JOIN [{SCHEMA}].executions e ON i.instance_id = e.instance_id 
                    AND i.current_execution_id = e.execution_id
                JOIN #ids ids ON i.instance_id = ids.instance_id
                WHERE e.status = 'Running'
            )
            BEGIN
                THROW 50001, 'Cannot delete running instances without force=true', 1;
            END;
            
            -- Check for orphans: children whose parents would be deleted but children are not in the delete list
            IF EXISTS (
                SELECT 1 FROM [{SCHEMA}].instances child
                JOIN #ids parent_ids ON child.parent_instance_id = parent_ids.instance_id
                WHERE child.instance_id NOT IN (SELECT instance_id FROM #ids)
            )
            BEGIN
                THROW 50002, 'Cannot delete instances that have children not included in the delete list', 1;
            END;
        END;
        
        -- Delete locks
        DELETE FROM [{SCHEMA}].instance_locks WHERE instance_id IN (SELECT instance_id FROM #ids);
        
        -- Delete queue items
        DELETE FROM [{SCHEMA}].orchestrator_queue WHERE instance_id IN (SELECT instance_id FROM #ids);
        SET @queue_deleted = @queue_deleted + @@ROWCOUNT;
        
        DELETE FROM [{SCHEMA}].worker_queue WHERE instance_id IN (SELECT instance_id FROM #ids);
        SET @queue_deleted = @queue_deleted + @@ROWCOUNT;
        
        -- Delete history
        DELETE FROM [{SCHEMA}].history WHERE instance_id IN (SELECT instance_id FROM #ids);
        SET @events_deleted = @@ROWCOUNT;
        
        -- Delete executions
        DELETE FROM [{SCHEMA}].executions WHERE instance_id IN (SELECT instance_id FROM #ids);
        SET @executions_deleted = @@ROWCOUNT;
        
        -- Delete instances
        DELETE FROM [{SCHEMA}].instances WHERE instance_id IN (SELECT instance_id FROM #ids);
        SET @instances_deleted = @@ROWCOUNT;
        
        DROP TABLE #ids;
        
        COMMIT TRANSACTION;
        
        SELECT @instances_deleted AS instances_deleted,
               @executions_deleted AS executions_deleted,
               @events_deleted AS events_deleted,
               @queue_deleted AS queue_messages_deleted;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        IF OBJECT_ID('tempdb..#ids') IS NOT NULL DROP TABLE #ids;
        THROW;
    END CATCH;
END;
GO

-- ============================================================================
-- Delete Instance Bulk (with filter)
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_delete_instance_bulk', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_delete_instance_bulk;
GO

CREATE PROCEDURE [{SCHEMA}].sp_delete_instance_bulk
    @filter NVARCHAR(MAX)  -- JSON object
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @completed_before NVARCHAR(50) = JSON_VALUE(@filter, '$.completed_before');
    DECLARE @limit INT = CAST(COALESCE(JSON_VALUE(@filter, '$.limit'), '1000') AS INT);
    
    DECLARE @instances_deleted BIGINT = 0;
    DECLARE @executions_deleted BIGINT = 0;
    DECLARE @events_deleted BIGINT = 0;
    DECLARE @queue_deleted BIGINT = 0;
    
    BEGIN TRANSACTION;
    BEGIN TRY
        -- Find terminal instances matching filter
        CREATE TABLE #ids (instance_id NVARCHAR(255));
        
        INSERT INTO #ids
        SELECT TOP (@limit) i.instance_id
        FROM [{SCHEMA}].instances i
        JOIN [{SCHEMA}].executions e ON i.instance_id = e.instance_id 
            AND i.current_execution_id = e.execution_id
        WHERE e.status IN ('Completed', 'Failed', 'ContinuedAsNew')
          AND i.parent_instance_id IS NULL  -- Root instances only
          AND (@completed_before IS NULL OR e.completed_at < CAST(@completed_before AS DATETIME2));
        
        -- Delete cascade (same as atomic delete)
        DELETE FROM [{SCHEMA}].instance_locks WHERE instance_id IN (SELECT instance_id FROM #ids);
        
        DELETE FROM [{SCHEMA}].orchestrator_queue WHERE instance_id IN (SELECT instance_id FROM #ids);
        SET @queue_deleted = @queue_deleted + @@ROWCOUNT;
        
        DELETE FROM [{SCHEMA}].worker_queue WHERE instance_id IN (SELECT instance_id FROM #ids);
        SET @queue_deleted = @queue_deleted + @@ROWCOUNT;
        
        DELETE FROM [{SCHEMA}].history WHERE instance_id IN (SELECT instance_id FROM #ids);
        SET @events_deleted = @@ROWCOUNT;
        
        DELETE FROM [{SCHEMA}].executions WHERE instance_id IN (SELECT instance_id FROM #ids);
        SET @executions_deleted = @@ROWCOUNT;
        
        DELETE FROM [{SCHEMA}].instances WHERE instance_id IN (SELECT instance_id FROM #ids);
        SET @instances_deleted = @@ROWCOUNT;
        
        DROP TABLE #ids;
        
        COMMIT TRANSACTION;
        
        SELECT @instances_deleted AS instances_deleted,
               @executions_deleted AS executions_deleted,
               @events_deleted AS events_deleted,
               @queue_deleted AS queue_messages_deleted;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        IF OBJECT_ID('tempdb..#ids') IS NOT NULL DROP TABLE #ids;
        THROW;
    END CATCH;
END;
GO

-- ============================================================================
-- Prune Executions
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_prune_executions', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_prune_executions;
GO

CREATE PROCEDURE [{SCHEMA}].sp_prune_executions
    @instance_id NVARCHAR(255),
    @options NVARCHAR(MAX)  -- JSON object with keep_last, completed_before
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @keep_last INT = CAST(COALESCE(JSON_VALUE(@options, '$.keep_last'), '1') AS INT);
    DECLARE @completed_before NVARCHAR(50) = JSON_VALUE(@options, '$.completed_before');
    DECLARE @current_exec BIGINT;
    DECLARE @executions_deleted BIGINT = 0;
    DECLARE @events_deleted BIGINT = 0;
    
    -- Get current execution (never prune)
    SELECT @current_exec = current_execution_id FROM [{SCHEMA}].instances WHERE instance_id = @instance_id;
    
    IF @current_exec IS NULL
    BEGIN
        THROW 50001, 'Instance not found', 1;
    END;
    
    BEGIN TRANSACTION;
    BEGIN TRY
        -- Find executions to prune
        CREATE TABLE #prune_execs (execution_id BIGINT);
        
        INSERT INTO #prune_execs
        SELECT e.execution_id
        FROM [{SCHEMA}].executions e
        WHERE e.instance_id = @instance_id
          AND e.execution_id != @current_exec  -- Never prune current
          AND e.execution_id NOT IN (
              -- Keep last N
              SELECT TOP (@keep_last) execution_id
              FROM [{SCHEMA}].executions
              WHERE instance_id = @instance_id
              ORDER BY execution_id DESC
          )
          AND (@completed_before IS NULL OR e.completed_at < CAST(@completed_before AS DATETIME2));
        
        -- Delete history for pruned executions
        DELETE FROM [{SCHEMA}].history 
        WHERE instance_id = @instance_id 
          AND execution_id IN (SELECT execution_id FROM #prune_execs);
        SET @events_deleted = @@ROWCOUNT;
        
        -- Delete executions
        DELETE FROM [{SCHEMA}].executions 
        WHERE instance_id = @instance_id 
          AND execution_id IN (SELECT execution_id FROM #prune_execs);
        SET @executions_deleted = @@ROWCOUNT;
        
        DROP TABLE #prune_execs;
        
        COMMIT TRANSACTION;
        
        SELECT 1 AS instances_processed,
               @executions_deleted AS executions_deleted,
               @events_deleted AS events_deleted;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        IF OBJECT_ID('tempdb..#prune_execs') IS NOT NULL DROP TABLE #prune_execs;
        THROW;
    END CATCH;
END;
GO

-- ============================================================================
-- Prune Executions Bulk
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_prune_executions_bulk', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_prune_executions_bulk;
GO

CREATE PROCEDURE [{SCHEMA}].sp_prune_executions_bulk
    @filter NVARCHAR(MAX),   -- JSON object
    @options NVARCHAR(MAX)   -- JSON object with keep_last
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @keep_last INT = CAST(COALESCE(JSON_VALUE(@options, '$.keep_last'), '1') AS INT);
    DECLARE @limit INT = CAST(COALESCE(JSON_VALUE(@filter, '$.limit'), '100') AS INT);
    DECLARE @instances_processed BIGINT = 0;
    DECLARE @executions_deleted BIGINT = 0;
    DECLARE @events_deleted BIGINT = 0;
    
    BEGIN TRANSACTION;
    BEGIN TRY
        -- Parse instance_ids from filter if provided
        CREATE TABLE #filter_ids (instance_id NVARCHAR(255));
        IF JSON_QUERY(@filter, '$.instance_ids') IS NOT NULL
        BEGIN
            INSERT INTO #filter_ids
            SELECT value FROM OPENJSON(@filter, '$.instance_ids');
        END;
        
        -- Find instances with multiple executions
        CREATE TABLE #instances (instance_id NVARCHAR(255), current_execution_id BIGINT);
        
        INSERT INTO #instances
        SELECT TOP (@limit) i.instance_id, i.current_execution_id
        FROM [{SCHEMA}].instances i
        WHERE EXISTS (
            SELECT 1 FROM [{SCHEMA}].executions e
            WHERE e.instance_id = i.instance_id
            GROUP BY e.instance_id
            HAVING COUNT(*) > @keep_last
        )
        -- Filter by instance_ids if provided
        AND (NOT EXISTS (SELECT 1 FROM #filter_ids) OR i.instance_id IN (SELECT instance_id FROM #filter_ids));
        
        SET @instances_processed = @@ROWCOUNT;
        
        -- Find executions to prune (per instance, keep current + last N)
        CREATE TABLE #prune_execs (instance_id NVARCHAR(255), execution_id BIGINT);
        
        INSERT INTO #prune_execs
        SELECT e.instance_id, e.execution_id
        FROM [{SCHEMA}].executions e
        JOIN #instances i ON e.instance_id = i.instance_id
        WHERE e.execution_id != i.current_execution_id  -- Never prune current
          AND e.execution_id NOT IN (
              SELECT execution_id
              FROM (
                  SELECT execution_id, ROW_NUMBER() OVER (PARTITION BY instance_id ORDER BY execution_id DESC) AS rn
                  FROM [{SCHEMA}].executions e2
                  WHERE e2.instance_id = e.instance_id
              ) ranked
              WHERE rn <= @keep_last
          );
        
        -- Delete history
        DELETE h FROM [{SCHEMA}].history h
        JOIN #prune_execs p ON h.instance_id = p.instance_id AND h.execution_id = p.execution_id;
        SET @events_deleted = @@ROWCOUNT;
        
        -- Delete executions
        DELETE e FROM [{SCHEMA}].executions e
        JOIN #prune_execs p ON e.instance_id = p.instance_id AND e.execution_id = p.execution_id;
        SET @executions_deleted = @@ROWCOUNT;
        
        DROP TABLE #filter_ids;
        DROP TABLE #instances;
        DROP TABLE #prune_execs;
        
        COMMIT TRANSACTION;
        
        SELECT @instances_processed AS instances_processed,
               @executions_deleted AS executions_deleted,
               @events_deleted AS events_deleted;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        IF OBJECT_ID('tempdb..#filter_ids') IS NOT NULL DROP TABLE #filter_ids;
        IF OBJECT_ID('tempdb..#instances') IS NOT NULL DROP TABLE #instances;
        IF OBJECT_ID('tempdb..#prune_execs') IS NOT NULL DROP TABLE #prune_execs;
        THROW;
    END CATCH;
END;
GO
