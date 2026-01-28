-- Migration 0002: Create stored procedures for duroxide-sql
-- T-SQL stored procedures for atomic operations

-- ============================================================================
-- Cleanup procedure (for testing only)
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_cleanup_schema', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_cleanup_schema;
GO

CREATE PROCEDURE [{SCHEMA}].sp_cleanup_schema
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Drop tables
    IF OBJECT_ID('[{SCHEMA}].instance_locks', 'U') IS NOT NULL DROP TABLE [{SCHEMA}].instance_locks;
    IF OBJECT_ID('[{SCHEMA}].history', 'U') IS NOT NULL DROP TABLE [{SCHEMA}].history;
    IF OBJECT_ID('[{SCHEMA}].orchestrator_queue', 'U') IS NOT NULL DROP TABLE [{SCHEMA}].orchestrator_queue;
    IF OBJECT_ID('[{SCHEMA}].worker_queue', 'U') IS NOT NULL DROP TABLE [{SCHEMA}].worker_queue;
    IF OBJECT_ID('[{SCHEMA}].executions', 'U') IS NOT NULL DROP TABLE [{SCHEMA}].executions;
    IF OBJECT_ID('[{SCHEMA}].instances', 'U') IS NOT NULL DROP TABLE [{SCHEMA}].instances;
    IF OBJECT_ID('[{SCHEMA}]._duroxide_migrations', 'U') IS NOT NULL DROP TABLE [{SCHEMA}]._duroxide_migrations;
END;
GO

-- ============================================================================
-- Orchestrator Queue: Enqueue
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_enqueue_orchestrator_work', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_enqueue_orchestrator_work;
GO

CREATE PROCEDURE [{SCHEMA}].sp_enqueue_orchestrator_work
    @instance_id NVARCHAR(255),
    @work_item NVARCHAR(MAX),
    @visible_at BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @visible_at_dt DATETIME2 = DATEADD(MILLISECOND, @visible_at % 1000, 
                                        DATEADD(SECOND, @visible_at / 1000, '1970-01-01'));
    
    INSERT INTO [{SCHEMA}].orchestrator_queue (instance_id, work_item, visible_at, created_at)
    VALUES (@instance_id, @work_item, @visible_at_dt, SYSUTCDATETIME());
END;
GO

-- ============================================================================
-- Orchestrator Queue: Fetch and Lock
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_fetch_orchestration_item', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_fetch_orchestration_item;
GO

CREATE PROCEDURE [{SCHEMA}].sp_fetch_orchestration_item
    @now_ms BIGINT,
    @lock_timeout_ms BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @instance_id NVARCHAR(255);
    DECLARE @lock_token NVARCHAR(255);
    DECLARE @locked_until BIGINT;
    DECLARE @orchestration_name NVARCHAR(255);
    DECLARE @orchestration_version NVARCHAR(255);
    DECLARE @current_execution_id BIGINT;
    DECLARE @max_attempt_count INT = 0;
    
    BEGIN TRANSACTION;
    
    -- Find a candidate instance (not locked or lock expired)
    SELECT TOP 1 @instance_id = q.instance_id
    FROM [{SCHEMA}].orchestrator_queue q WITH (READPAST, ROWLOCK, UPDLOCK)
    LEFT JOIN [{SCHEMA}].instance_locks il ON q.instance_id = il.instance_id
    WHERE q.visible_at <= SYSUTCDATETIME()
      AND (il.instance_id IS NULL OR il.locked_until <= @now_ms)
    ORDER BY q.id;
    
    IF @instance_id IS NULL
    BEGIN
        ROLLBACK TRANSACTION;
        RETURN;
    END;
    
    -- Generate lock token and set lock expiration
    SET @lock_token = 'lock_' + CONVERT(NVARCHAR(36), NEWID());
    SET @locked_until = @now_ms + @lock_timeout_ms;
    
    -- Acquire instance lock (upsert)
    MERGE [{SCHEMA}].instance_locks AS target
    USING (SELECT @instance_id AS instance_id) AS source
    ON target.instance_id = source.instance_id
    WHEN MATCHED THEN
        UPDATE SET lock_token = @lock_token, locked_until = @locked_until
    WHEN NOT MATCHED THEN
        INSERT (instance_id, lock_token, locked_until)
        VALUES (@instance_id, @lock_token, @locked_until);
    
    -- Tag messages with lock token and increment attempt count
    UPDATE [{SCHEMA}].orchestrator_queue
    SET lock_token = @lock_token,
        locked_until = @locked_until,
        attempt_count = attempt_count + 1
    WHERE instance_id = @instance_id
      AND visible_at <= SYSUTCDATETIME();
    
    -- Get max attempt count from tagged messages
    SELECT @max_attempt_count = MAX(attempt_count)
    FROM [{SCHEMA}].orchestrator_queue
    WHERE lock_token = @lock_token;
    
    -- Get instance metadata
    SELECT @orchestration_name = orchestration_name,
           @orchestration_version = orchestration_version,
           @current_execution_id = current_execution_id
    FROM [{SCHEMA}].instances
    WHERE instance_id = @instance_id;
    
    -- If instance doesn't exist, try to extract from StartOrchestration work item
    IF @orchestration_name IS NULL
    BEGIN
        SELECT TOP 1 
            @orchestration_name = JSON_VALUE(work_item, '$.StartOrchestration.orchestration'),
            @orchestration_version = JSON_VALUE(work_item, '$.StartOrchestration.version')
        FROM [{SCHEMA}].orchestrator_queue
        WHERE lock_token = @lock_token
          AND JSON_VALUE(work_item, '$.StartOrchestration.orchestration') IS NOT NULL
        ORDER BY id;
    END;
    
    -- Default values if still not found
    IF @orchestration_name IS NULL SET @orchestration_name = 'Unknown';
    IF @orchestration_version IS NULL SET @orchestration_version = 'unknown';
    IF @current_execution_id IS NULL SET @current_execution_id = 1;
    
    -- Get messages as JSON
    DECLARE @messages NVARCHAR(MAX);
    SELECT @messages = (
        SELECT work_item
        FROM [{SCHEMA}].orchestrator_queue
        WHERE lock_token = @lock_token
        ORDER BY id
        FOR JSON PATH
    );
    IF @messages IS NULL SET @messages = '[]';
    
    -- Unwrap the JSON array of work_items
    SELECT @messages = '[' + STRING_AGG(JSON_VALUE(value, '$.work_item'), ',') + ']'
    FROM OPENJSON(@messages);
    IF @messages IS NULL OR @messages = '[null]' SET @messages = '[]';
    
    -- Get history as JSON
    DECLARE @history NVARCHAR(MAX);
    SELECT @history = (
        SELECT event_data
        FROM [{SCHEMA}].history
        WHERE instance_id = @instance_id
          AND execution_id = @current_execution_id
        ORDER BY event_id
        FOR JSON PATH
    );
    IF @history IS NULL SET @history = '[]';
    
    -- Unwrap the JSON array of event_data
    SELECT @history = '[' + STRING_AGG(JSON_VALUE(value, '$.event_data'), ',') + ']'
    FROM OPENJSON(@history);
    IF @history IS NULL OR @history = '[null]' SET @history = '[]';
    
    COMMIT TRANSACTION;
    
    -- Return result
    SELECT 
        @instance_id AS instance_id,
        @orchestration_name AS orchestration_name,
        @orchestration_version AS orchestration_version,
        @current_execution_id AS execution_id,
        @history AS history,
        @messages AS messages,
        @lock_token AS lock_token,
        @max_attempt_count AS attempt_count;
END;
GO

-- ============================================================================
-- Orchestrator Queue: Acknowledge (Atomic Commit)
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_ack_orchestration_item', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_ack_orchestration_item;
GO

CREATE PROCEDURE [{SCHEMA}].sp_ack_orchestration_item
    @lock_token NVARCHAR(255),
    @execution_id BIGINT,
    @history_delta NVARCHAR(MAX),      -- JSON array
    @worker_items NVARCHAR(MAX),        -- JSON array
    @orchestrator_items NVARCHAR(MAX),  -- JSON array
    @metadata NVARCHAR(MAX),            -- JSON object
    @cancelled_activities NVARCHAR(MAX), -- JSON array
    @now_ms BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @instance_id NVARCHAR(255);
    DECLARE @orchestration_name NVARCHAR(255);
    DECLARE @orchestration_version NVARCHAR(255);
    DECLARE @status NVARCHAR(50);
    DECLARE @output NVARCHAR(MAX);
    DECLARE @parent_instance_id NVARCHAR(255);
    
    BEGIN TRANSACTION;
    BEGIN TRY
        -- Step 1: Validate lock token
        SELECT @instance_id = instance_id
        FROM [{SCHEMA}].instance_locks WITH (ROWLOCK, UPDLOCK)
        WHERE lock_token = @lock_token AND locked_until > @now_ms;
        
        IF @instance_id IS NULL
        BEGIN
            THROW 50001, 'Invalid lock token', 1;
        END;
        
        -- Step 2: Extract metadata
        SET @orchestration_name = JSON_VALUE(@metadata, '$.orchestration_name');
        SET @orchestration_version = JSON_VALUE(@metadata, '$.orchestration_version');
        SET @status = JSON_VALUE(@metadata, '$.status');
        SET @output = JSON_VALUE(@metadata, '$.output');
        SET @parent_instance_id = JSON_VALUE(@metadata, '$.parent_instance_id');
        
        -- Step 3: Create or update instance (always do the merge to ensure instance exists)
        MERGE [{SCHEMA}].instances AS target
        USING (SELECT @instance_id AS instance_id) AS source
        ON target.instance_id = source.instance_id
        WHEN MATCHED THEN
            UPDATE SET 
                orchestration_name = COALESCE(@orchestration_name, target.orchestration_name),
                orchestration_version = COALESCE(@orchestration_version, target.orchestration_version),
                current_execution_id = CASE WHEN @execution_id > target.current_execution_id 
                                            THEN @execution_id ELSE target.current_execution_id END,
                parent_instance_id = COALESCE(@parent_instance_id, target.parent_instance_id),
                updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (instance_id, orchestration_name, orchestration_version, current_execution_id, parent_instance_id, created_at, updated_at)
            VALUES (@instance_id, COALESCE(@orchestration_name, 'Unknown'), COALESCE(@orchestration_version, 'unknown'), @execution_id, @parent_instance_id, SYSUTCDATETIME(), SYSUTCDATETIME());
        
        -- Step 4: Create or update execution
        MERGE [{SCHEMA}].executions AS target
        USING (SELECT @instance_id AS instance_id, @execution_id AS execution_id) AS source
        ON target.instance_id = source.instance_id AND target.execution_id = source.execution_id
        WHEN MATCHED AND @status IS NOT NULL THEN
            UPDATE SET 
                status = @status,
                output = @output,
                completed_at = CASE WHEN @status IN ('Completed', 'Failed', 'ContinuedAsNew') 
                                    THEN SYSUTCDATETIME() ELSE completed_at END
        WHEN NOT MATCHED THEN
            INSERT (instance_id, execution_id, status, output, started_at)
            VALUES (@instance_id, @execution_id, COALESCE(@status, 'Running'), @output, SYSUTCDATETIME());
        
        -- Step 5: Append history events
        IF @history_delta IS NOT NULL AND @history_delta != '[]'
        BEGIN
            INSERT INTO [{SCHEMA}].history (instance_id, execution_id, event_id, event_type, event_data, created_at, updated_at)
            SELECT 
                @instance_id,
                @execution_id,
                CAST(JSON_VALUE(value, '$.event_id') AS BIGINT),
                JSON_VALUE(value, '$.event_type'),
                value,
                SYSUTCDATETIME(),
                SYSUTCDATETIME()
            FROM OPENJSON(@history_delta);
        END;
        
        -- Step 6: Enqueue worker items
        IF @worker_items IS NOT NULL AND @worker_items != '[]'
        BEGIN
            INSERT INTO [{SCHEMA}].worker_queue (work_item, visible_at, instance_id, execution_id, activity_id)
            SELECT 
                value,
                @now_ms,
                JSON_VALUE(value, '$.ActivityExecute.instance'),
                CAST(JSON_VALUE(value, '$.ActivityExecute.execution_id') AS BIGINT),
                CAST(JSON_VALUE(value, '$.ActivityExecute.id') AS BIGINT)
            FROM OPENJSON(@worker_items);
        END;
        
        -- Step 7: Enqueue orchestrator items
        IF @orchestrator_items IS NOT NULL AND @orchestrator_items != '[]'
        BEGIN
            DECLARE @item_instance NVARCHAR(255);
            DECLARE @fire_at_ms BIGINT;
            DECLARE @visible_at DATETIME2;
            
            -- Use cursor for items that may have delayed visibility (timers)
            DECLARE item_cursor CURSOR FOR
                SELECT 
                    COALESCE(
                        JSON_VALUE(value, '$.StartOrchestration.instance'),
                        JSON_VALUE(value, '$.ActivityCompleted.instance'),
                        JSON_VALUE(value, '$.ActivityFailed.instance'),
                        JSON_VALUE(value, '$.TimerFired.instance'),
                        JSON_VALUE(value, '$.ExternalRaised.instance'),
                        JSON_VALUE(value, '$.SubOrchCompleted.instance'),
                        JSON_VALUE(value, '$.SubOrchFailed.instance'),
                        JSON_VALUE(value, '$.ContinueAsNew.instance'),
                        JSON_VALUE(value, '$.CancelInstance.instance')
                    ) AS item_instance,
                    JSON_VALUE(value, '$.TimerFired.fire_at_ms') AS fire_at_ms,
                    value
                FROM OPENJSON(@orchestrator_items);
            
            OPEN item_cursor;
            DECLARE @item_value NVARCHAR(MAX);
            
            FETCH NEXT FROM item_cursor INTO @item_instance, @fire_at_ms, @item_value;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @fire_at_ms IS NOT NULL
                    SET @visible_at = DATEADD(MILLISECOND, CAST(@fire_at_ms AS BIGINT) % 1000, 
                                      DATEADD(SECOND, CAST(@fire_at_ms AS BIGINT) / 1000, '1970-01-01'));
                ELSE
                    SET @visible_at = SYSUTCDATETIME();
                
                INSERT INTO [{SCHEMA}].orchestrator_queue (instance_id, work_item, visible_at, created_at)
                VALUES (@item_instance, @item_value, @visible_at, SYSUTCDATETIME());
                
                FETCH NEXT FROM item_cursor INTO @item_instance, @fire_at_ms, @item_value;
            END;
            
            CLOSE item_cursor;
            DEALLOCATE item_cursor;
        END;
        
        -- Step 8: Delete cancelled activities from worker queue (lock stealing)
        IF @cancelled_activities IS NOT NULL AND @cancelled_activities != '[]'
        BEGIN
            DELETE wq
            FROM [{SCHEMA}].worker_queue wq
            INNER JOIN OPENJSON(@cancelled_activities) ca
                ON wq.instance_id = JSON_VALUE(ca.value, '$.instance')
                AND wq.execution_id = CAST(JSON_VALUE(ca.value, '$.execution_id') AS BIGINT)
                AND wq.activity_id = CAST(JSON_VALUE(ca.value, '$.activity_id') AS BIGINT);
        END;
        
        -- Step 9: Delete processed messages
        DELETE FROM [{SCHEMA}].orchestrator_queue WHERE lock_token = @lock_token;
        
        -- Step 10: Release instance lock
        DELETE FROM [{SCHEMA}].instance_locks WHERE lock_token = @lock_token;
        
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO

-- ============================================================================
-- Orchestrator Queue: Abandon
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_abandon_orchestration_item', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_abandon_orchestration_item;
GO

CREATE PROCEDURE [{SCHEMA}].sp_abandon_orchestration_item
    @lock_token NVARCHAR(255),
    @now_ms BIGINT,
    @delay_ms BIGINT = NULL,
    @ignore_attempt BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @instance_id NVARCHAR(255);
    
    BEGIN TRANSACTION;
    
    -- Get instance from lock
    SELECT @instance_id = instance_id
    FROM [{SCHEMA}].instance_locks
    WHERE lock_token = @lock_token;
    
    IF @instance_id IS NULL
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 50001, 'Invalid lock token', 1;
    END;
    
    -- Release messages back to queue
    IF @delay_ms IS NOT NULL AND @delay_ms > 0
    BEGIN
        DECLARE @visible_at DATETIME2 = DATEADD(MILLISECOND, @delay_ms % 1000, 
                                        DATEADD(SECOND, @delay_ms / 1000, SYSUTCDATETIME()));
        
        IF @ignore_attempt = 1
        BEGIN
            UPDATE [{SCHEMA}].orchestrator_queue
            SET lock_token = NULL, locked_until = NULL, visible_at = @visible_at,
                attempt_count = CASE WHEN attempt_count > 0 THEN attempt_count - 1 ELSE 0 END
            WHERE lock_token = @lock_token;
        END
        ELSE
        BEGIN
            UPDATE [{SCHEMA}].orchestrator_queue
            SET lock_token = NULL, locked_until = NULL, visible_at = @visible_at
            WHERE lock_token = @lock_token;
        END;
    END
    ELSE
    BEGIN
        IF @ignore_attempt = 1
        BEGIN
            UPDATE [{SCHEMA}].orchestrator_queue
            SET lock_token = NULL, locked_until = NULL,
                attempt_count = CASE WHEN attempt_count > 0 THEN attempt_count - 1 ELSE 0 END
            WHERE lock_token = @lock_token;
        END
        ELSE
        BEGIN
            UPDATE [{SCHEMA}].orchestrator_queue
            SET lock_token = NULL, locked_until = NULL
            WHERE lock_token = @lock_token;
        END;
    END;
    
    -- Release instance lock
    DELETE FROM [{SCHEMA}].instance_locks WHERE lock_token = @lock_token;
    
    COMMIT TRANSACTION;
END;
GO

-- ============================================================================
-- Orchestrator Queue: Renew Lock
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_renew_orchestration_lock', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_renew_orchestration_lock;
GO

CREATE PROCEDURE [{SCHEMA}].sp_renew_orchestration_lock
    @lock_token NVARCHAR(255),
    @now_ms BIGINT,
    @extend_ms BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @locked_until BIGINT = @now_ms + @extend_ms;
    DECLARE @rows_affected INT;
    
    UPDATE [{SCHEMA}].instance_locks
    SET locked_until = @locked_until
    WHERE lock_token = @lock_token AND locked_until > @now_ms;
    
    SET @rows_affected = @@ROWCOUNT;
    
    IF @rows_affected = 0
    BEGIN
        THROW 50001, 'Lock token invalid, expired, or already released', 1;
    END;
    
    UPDATE [{SCHEMA}].orchestrator_queue
    SET locked_until = @locked_until
    WHERE lock_token = @lock_token;
END;
GO

-- ============================================================================
-- Worker Queue: Enqueue
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_enqueue_worker_work', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_enqueue_worker_work;
GO

CREATE PROCEDURE [{SCHEMA}].sp_enqueue_worker_work
    @work_item NVARCHAR(MAX),
    @visible_at BIGINT,
    @instance_id NVARCHAR(255) = NULL,
    @execution_id BIGINT = NULL,
    @activity_id BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO [{SCHEMA}].worker_queue (work_item, visible_at, instance_id, execution_id, activity_id)
    VALUES (@work_item, @visible_at, @instance_id, @execution_id, @activity_id);
END;
GO

-- ============================================================================
-- Worker Queue: Fetch and Lock
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_fetch_work_item', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_fetch_work_item;
GO

CREATE PROCEDURE [{SCHEMA}].sp_fetch_work_item
    @now_ms BIGINT,
    @lock_timeout_ms BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @id BIGINT;
    DECLARE @work_item NVARCHAR(MAX);
    DECLARE @lock_token NVARCHAR(255);
    DECLARE @locked_until BIGINT;
    DECLARE @attempt_count INT;
    
    BEGIN TRANSACTION;
    
    -- Find and lock next available item
    SELECT TOP 1 @id = id
    FROM [{SCHEMA}].worker_queue WITH (READPAST, ROWLOCK, UPDLOCK)
    WHERE visible_at <= @now_ms
      AND (lock_token IS NULL OR locked_until <= @now_ms)
    ORDER BY id;
    
    IF @id IS NULL
    BEGIN
        ROLLBACK TRANSACTION;
        RETURN;
    END;
    
    -- Generate lock token
    SET @lock_token = 'lock_' + CONVERT(NVARCHAR(36), NEWID());
    SET @locked_until = @now_ms + @lock_timeout_ms;
    
    -- Lock the item and increment attempt count
    UPDATE [{SCHEMA}].worker_queue
    SET lock_token = @lock_token,
        locked_until = @locked_until,
        attempt_count = attempt_count + 1
    WHERE id = @id;
    
    -- Get the locked item
    SELECT @work_item = work_item, @attempt_count = attempt_count
    FROM [{SCHEMA}].worker_queue
    WHERE id = @id;
    
    COMMIT TRANSACTION;
    
    -- Return result
    SELECT @work_item AS work_item, @lock_token AS lock_token, @attempt_count AS attempt_count;
END;
GO

-- ============================================================================
-- Worker Queue: Acknowledge
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_ack_worker', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_ack_worker;
GO

CREATE PROCEDURE [{SCHEMA}].sp_ack_worker
    @lock_token NVARCHAR(255),
    @instance_id NVARCHAR(255) = NULL,
    @completion_json NVARCHAR(MAX) = NULL,
    @now_ms BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @rows_affected INT;
    
    BEGIN TRANSACTION;
    BEGIN TRY
        -- Delete the worker queue item
        DELETE FROM [{SCHEMA}].worker_queue WHERE lock_token = @lock_token;
        SET @rows_affected = @@ROWCOUNT;
        
        IF @rows_affected = 0
        BEGIN
            THROW 50001, 'Worker queue item not found or already processed', 1;
        END;
        
        -- Enqueue completion if provided
        IF @completion_json IS NOT NULL AND @instance_id IS NOT NULL
        BEGIN
            INSERT INTO [{SCHEMA}].orchestrator_queue (instance_id, work_item, visible_at, created_at)
            VALUES (@instance_id, @completion_json, SYSUTCDATETIME(), SYSUTCDATETIME());
        END;
        
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO

-- ============================================================================
-- Worker Queue: Abandon
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_abandon_work_item', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_abandon_work_item;
GO

CREATE PROCEDURE [{SCHEMA}].sp_abandon_work_item
    @lock_token NVARCHAR(255),
    @now_ms BIGINT,
    @delay_ms BIGINT = NULL,
    @ignore_attempt BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @rows_affected INT;
    DECLARE @visible_at BIGINT;
    
    IF @delay_ms IS NOT NULL AND @delay_ms > 0
        SET @visible_at = @now_ms + @delay_ms;
    ELSE
        SET @visible_at = @now_ms;
    
    IF @ignore_attempt = 1
    BEGIN
        UPDATE [{SCHEMA}].worker_queue
        SET lock_token = NULL, locked_until = NULL, visible_at = @visible_at,
            attempt_count = CASE WHEN attempt_count > 0 THEN attempt_count - 1 ELSE 0 END
        WHERE lock_token = @lock_token;
    END
    ELSE
    BEGIN
        UPDATE [{SCHEMA}].worker_queue
        SET lock_token = NULL, locked_until = NULL, visible_at = @visible_at
        WHERE lock_token = @lock_token;
    END;
    
    SET @rows_affected = @@ROWCOUNT;
    
    IF @rows_affected = 0
    BEGIN
        THROW 50001, 'Invalid lock token or already acked', 1;
    END;
END;
GO

-- ============================================================================
-- Worker Queue: Renew Lock
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_renew_work_item_lock', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_renew_work_item_lock;
GO

CREATE PROCEDURE [{SCHEMA}].sp_renew_work_item_lock
    @lock_token NVARCHAR(255),
    @now_ms BIGINT,
    @extend_ms BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @locked_until BIGINT = @now_ms + @extend_ms;
    DECLARE @rows_affected INT;
    
    UPDATE [{SCHEMA}].worker_queue
    SET locked_until = @locked_until
    WHERE lock_token = @lock_token AND locked_until > @now_ms;
    
    SET @rows_affected = @@ROWCOUNT;
    
    IF @rows_affected = 0
    BEGIN
        THROW 50001, 'Lock token invalid, expired, or already released', 1;
    END;
END;
GO

-- ============================================================================
-- History: Fetch (Latest Execution)
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_fetch_history', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_fetch_history;
GO

CREATE PROCEDURE [{SCHEMA}].sp_fetch_history
    @instance_id NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @execution_id BIGINT;
    
    SELECT @execution_id = COALESCE(MAX(execution_id), 1)
    FROM [{SCHEMA}].executions
    WHERE instance_id = @instance_id;
    
    SELECT event_data
    FROM [{SCHEMA}].history
    WHERE instance_id = @instance_id
      AND execution_id = @execution_id
    ORDER BY event_id;
END;
GO

-- ============================================================================
-- History: Fetch with Execution
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_fetch_history_with_execution', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_fetch_history_with_execution;
GO

CREATE PROCEDURE [{SCHEMA}].sp_fetch_history_with_execution
    @instance_id NVARCHAR(255),
    @execution_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT event_data
    FROM [{SCHEMA}].history
    WHERE instance_id = @instance_id
      AND execution_id = @execution_id
    ORDER BY event_id;
END;
GO

-- ============================================================================
-- History: Append
-- ============================================================================
IF OBJECT_ID('[{SCHEMA}].sp_append_history', 'P') IS NOT NULL
    DROP PROCEDURE [{SCHEMA}].sp_append_history;
GO

CREATE PROCEDURE [{SCHEMA}].sp_append_history
    @instance_id NVARCHAR(255),
    @execution_id BIGINT,
    @events NVARCHAR(MAX)  -- JSON array
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @events IS NULL OR @events = '[]'
        RETURN;
    
    -- Insert events, ignoring duplicates
    INSERT INTO [{SCHEMA}].history (instance_id, execution_id, event_id, event_type, event_data, created_at, updated_at)
    SELECT 
        @instance_id,
        @execution_id,
        CAST(JSON_VALUE(value, '$.event_id') AS BIGINT),
        JSON_VALUE(value, '$.event_type'),
        value,
        SYSUTCDATETIME(),
        SYSUTCDATETIME()
    FROM OPENJSON(@events)
    WHERE NOT EXISTS (
        SELECT 1 FROM [{SCHEMA}].history h
        WHERE h.instance_id = @instance_id
          AND h.execution_id = @execution_id
          AND h.event_id = CAST(JSON_VALUE(value, '$.event_id') AS BIGINT)
    );
END;
GO
