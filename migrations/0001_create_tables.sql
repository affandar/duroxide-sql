-- Migration 0001: Create base tables for duroxide-sql
-- Creates the core schema for orchestration state management

-- 1. Orchestration instance metadata
IF NOT EXISTS (SELECT * FROM sys.tables t 
    JOIN sys.schemas s ON t.schema_id = s.schema_id 
    WHERE s.name = '{SCHEMA}' AND t.name = 'instances')
CREATE TABLE [{SCHEMA}].instances (
    instance_id NVARCHAR(255) NOT NULL PRIMARY KEY,
    orchestration_name NVARCHAR(255) NOT NULL,
    orchestration_version NVARCHAR(255) NULL,
    current_execution_id BIGINT NOT NULL DEFAULT 1,
    parent_instance_id NVARCHAR(255) NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 2. Execution records (one per execution_id per instance)
IF NOT EXISTS (SELECT * FROM sys.tables t 
    JOIN sys.schemas s ON t.schema_id = s.schema_id 
    WHERE s.name = '{SCHEMA}' AND t.name = 'executions')
CREATE TABLE [{SCHEMA}].executions (
    instance_id NVARCHAR(255) NOT NULL,
    execution_id BIGINT NOT NULL,
    status NVARCHAR(50) NOT NULL DEFAULT 'Running',
    output NVARCHAR(MAX) NULL,
    started_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    completed_at DATETIME2 NULL,
    PRIMARY KEY (instance_id, execution_id)
);
GO

-- 3. Event history (append-only log)
IF NOT EXISTS (SELECT * FROM sys.tables t 
    JOIN sys.schemas s ON t.schema_id = s.schema_id 
    WHERE s.name = '{SCHEMA}' AND t.name = 'history')
CREATE TABLE [{SCHEMA}].history (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    instance_id NVARCHAR(255) NOT NULL,
    execution_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,
    event_type NVARCHAR(100) NULL,
    event_data NVARCHAR(MAX) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_history_event UNIQUE (instance_id, execution_id, event_id)
);
GO

-- 4. Orchestrator work queue
IF NOT EXISTS (SELECT * FROM sys.tables t 
    JOIN sys.schemas s ON t.schema_id = s.schema_id 
    WHERE s.name = '{SCHEMA}' AND t.name = 'orchestrator_queue')
CREATE TABLE [{SCHEMA}].orchestrator_queue (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    instance_id NVARCHAR(255) NOT NULL,
    work_item NVARCHAR(MAX) NOT NULL,
    visible_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    lock_token NVARCHAR(255) NULL,
    locked_until BIGINT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 5. Worker (activity) queue
IF NOT EXISTS (SELECT * FROM sys.tables t 
    JOIN sys.schemas s ON t.schema_id = s.schema_id 
    WHERE s.name = '{SCHEMA}' AND t.name = 'worker_queue')
CREATE TABLE [{SCHEMA}].worker_queue (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    work_item NVARCHAR(MAX) NOT NULL,
    visible_at BIGINT NOT NULL,
    lock_token NVARCHAR(255) NULL,
    locked_until BIGINT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    instance_id NVARCHAR(255) NULL,
    execution_id BIGINT NULL,
    activity_id BIGINT NULL
);
GO

-- 6. Instance-level locks
IF NOT EXISTS (SELECT * FROM sys.tables t 
    JOIN sys.schemas s ON t.schema_id = s.schema_id 
    WHERE s.name = '{SCHEMA}' AND t.name = 'instance_locks')
CREATE TABLE [{SCHEMA}].instance_locks (
    instance_id NVARCHAR(255) NOT NULL PRIMARY KEY,
    lock_token NVARCHAR(255) NOT NULL,
    locked_until BIGINT NOT NULL
);
GO

-- Create indexes for performance

-- Orchestrator queue fetch optimization
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_orch_queue_fetch' AND object_id = OBJECT_ID('[{SCHEMA}].orchestrator_queue'))
CREATE INDEX idx_orch_queue_fetch ON [{SCHEMA}].orchestrator_queue (visible_at, instance_id) 
    WHERE lock_token IS NULL;
GO

-- Worker queue fetch optimization  
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_worker_queue_fetch' AND object_id = OBJECT_ID('[{SCHEMA}].worker_queue'))
CREATE INDEX idx_worker_queue_fetch ON [{SCHEMA}].worker_queue (visible_at) 
    WHERE lock_token IS NULL;
GO

-- Activity cancellation (lock stealing)
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_worker_activity' AND object_id = OBJECT_ID('[{SCHEMA}].worker_queue'))
CREATE INDEX idx_worker_activity ON [{SCHEMA}].worker_queue (instance_id, execution_id, activity_id);
GO

-- History lookup
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_history_lookup' AND object_id = OBJECT_ID('[{SCHEMA}].history'))
CREATE INDEX idx_history_lookup ON [{SCHEMA}].history (instance_id, execution_id, event_id);
GO

-- Parent-child relationships
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_instances_parent' AND object_id = OBJECT_ID('[{SCHEMA}].instances'))
CREATE INDEX idx_instances_parent ON [{SCHEMA}].instances (parent_instance_id);
GO
