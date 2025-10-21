// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package snowflakereceiver

import (
    "sync"
    "time"

    "go.uber.org/zap"
)

// CardinalityLimits defines max unique values per dimension
type CardinalityLimits struct {
    MaxUsers            int
    MaxSchemas          int
    MaxDatabases        int
    MaxQueryHashes      int
    MaxRoles            int
    MaxErrorCodes       int
    MaxPipeNames        int
    MaxTaskNames        int
    ResetInterval       time.Duration
}

// DefaultCardinalityLimits returns safe default limits
func DefaultCardinalityLimits() CardinalityLimits {
    return CardinalityLimits{
        MaxUsers:        500,
        MaxSchemas:      200,
        MaxDatabases:    100,
        MaxQueryHashes:  10000,
        MaxRoles:        200,
        MaxErrorCodes:   500,
        MaxPipeNames:    100,
        MaxTaskNames:    500,
        ResetInterval:   24 * time.Hour,
    }
}

// cardinalityTracker tracks unique values per dimension with circuit breaker
type cardinalityTracker struct {
    mu             sync.RWMutex
    limits         CardinalityLimits
    logger         *zap.Logger
    lastReset      time.Time
    
    // Track unique values
    users          map[string]struct{}
    schemas        map[string]struct{}
    databases      map[string]struct{}
    queryHashes    map[string]struct{}
    roles          map[string]struct{}
    errorCodes     map[string]struct{}
    pipeNames      map[string]struct{}
    taskNames      map[string]struct{}
    
    // Circuit breaker states
    usersBroken    bool
    schemasBroken  bool
    databasesBroken bool
    queryHashesBroken bool
    rolesBroken    bool
    errorCodesBroken bool
    pipeNamesBroken bool
    taskNamesBroken bool
    
    // Metrics for self-monitoring
    totalDroppedUsers   int64
    totalDroppedSchemas int64
    totalDroppedDbs     int64
}

func newCardinalityTracker(logger *zap.Logger, limits CardinalityLimits) *cardinalityTracker {
    return &cardinalityTracker{
        limits:      limits,
        logger:      logger,
        lastReset:   time.Now(),
        users:       make(map[string]struct{}),
        schemas:     make(map[string]struct{}),
        databases:   make(map[string]struct{}),
        queryHashes: make(map[string]struct{}),
        roles:       make(map[string]struct{}),
        errorCodes:  make(map[string]struct{}),
        pipeNames:   make(map[string]struct{}),
        taskNames:   make(map[string]struct{}),
    }
}

// checkReset resets counters if interval has passed
func (ct *cardinalityTracker) checkReset() {
    ct.mu.Lock()
    defer ct.mu.Unlock()
    
    if time.Since(ct.lastReset) > ct.limits.ResetInterval {
        ct.logger.Info("Resetting cardinality trackers",
            zap.Int("users_tracked", len(ct.users)),
            zap.Int("schemas_tracked", len(ct.schemas)),
            zap.Int("databases_tracked", len(ct.databases)),
            zap.Int64("total_users_dropped", ct.totalDroppedUsers),
            zap.Int64("total_schemas_dropped", ct.totalDroppedSchemas),
        )
        
        ct.users = make(map[string]struct{})
        ct.schemas = make(map[string]struct{})
        ct.databases = make(map[string]struct{})
        ct.queryHashes = make(map[string]struct{})
        ct.roles = make(map[string]struct{})
        ct.errorCodes = make(map[string]struct{})
        ct.pipeNames = make(map[string]struct{})
        ct.taskNames = make(map[string]struct{})
        
        ct.usersBroken = false
        ct.schemasBroken = false
        ct.databasesBroken = false
        ct.queryHashesBroken = false
        ct.rolesBroken = false
        ct.errorCodesBroken = false
        ct.pipeNamesBroken = false
        ct.taskNamesBroken = false
        
        ct.lastReset = time.Now()
    }
}

// trackUser returns the user dimension value with circuit breaker protection
func (ct *cardinalityTracker) trackUser(userName string) string {
    if userName == "" {
        return "unknown_user"
    }
    
    ct.checkReset()
    ct.mu.Lock()
    defer ct.mu.Unlock()
    
    if ct.usersBroken {
        ct.totalDroppedUsers++
        return "high_cardinality_user"
    }
    
    if _, exists := ct.users[userName]; exists {
        return userName
    }
    
    if len(ct.users) >= ct.limits.MaxUsers {
        ct.usersBroken = true
        ct.logger.Warn("User cardinality limit exceeded - circuit breaker activated",
            zap.Int("limit", ct.limits.MaxUsers),
            zap.Int("current", len(ct.users)),
            zap.String("new_user", userName),
        )
        ct.totalDroppedUsers++
        return "high_cardinality_user"
    }
    
    ct.users[userName] = struct{}{}
    return userName
}

// trackSchema returns the schema dimension value with circuit breaker protection
func (ct *cardinalityTracker) trackSchema(schemaName string) string {
    if schemaName == "" {
        return "unknown_schema"
    }
    
    ct.checkReset()
    ct.mu.Lock()
    defer ct.mu.Unlock()
    
    if ct.schemasBroken {
        ct.totalDroppedSchemas++
        return "high_cardinality_schema"
    }
    
    if _, exists := ct.schemas[schemaName]; exists {
        return schemaName
    }
    
    if len(ct.schemas) >= ct.limits.MaxSchemas {
        ct.schemasBroken = true
        ct.logger.Warn("Schema cardinality limit exceeded - circuit breaker activated",
            zap.Int("limit", ct.limits.MaxSchemas),
            zap.Int("current", len(ct.schemas)),
        )
        ct.totalDroppedSchemas++
        return "high_cardinality_schema"
    }
    
    ct.schemas[schemaName] = struct{}{}
    return schemaName
}

// trackDatabase returns the database dimension value with circuit breaker protection
func (ct *cardinalityTracker) trackDatabase(databaseName string) string {
    if databaseName == "" {
        return "unknown_database"
    }
    
    ct.checkReset()
    ct.mu.Lock()
    defer ct.mu.Unlock()
    
    if ct.databasesBroken {
        ct.totalDroppedDbs++
        return "high_cardinality_database"
    }
    
    if _, exists := ct.databases[databaseName]; exists {
        return databaseName
    }
    
    if len(ct.databases) >= ct.limits.MaxDatabases {
        ct.databasesBroken = true
        ct.logger.Warn("Database cardinality limit exceeded - circuit breaker activated",
            zap.Int("limit", ct.limits.MaxDatabases),
            zap.Int("current", len(ct.databases)),
        )
        ct.totalDroppedDbs++
        return "high_cardinality_database"
    }
    
    ct.databases[databaseName] = struct{}{}
    return databaseName
}

// trackRole returns the role dimension value with circuit breaker protection
func (ct *cardinalityTracker) trackRole(roleName string) string {
    if roleName == "" {
        return "unknown_role"
    }
    
    ct.checkReset()
    ct.mu.Lock()
    defer ct.mu.Unlock()
    
    if ct.rolesBroken {
        return "high_cardinality_role"
    }
    
    if _, exists := ct.roles[roleName]; exists {
        return roleName
    }
    
    if len(ct.roles) >= ct.limits.MaxRoles {
        ct.rolesBroken = true
        ct.logger.Warn("Role cardinality limit exceeded - circuit breaker activated",
            zap.Int("limit", ct.limits.MaxRoles),
            zap.Int("current", len(ct.roles)),
        )
        return "high_cardinality_role"
    }
    
    ct.roles[roleName] = struct{}{}
    return roleName
}

// trackQueryHash returns the query hash dimension value
func (ct *cardinalityTracker) trackQueryHash(queryHash string) string {
    if queryHash == "" {
        return "unknown_query_pattern"
    }
    
    ct.checkReset()
    ct.mu.Lock()
    defer ct.mu.Unlock()
    
    if ct.queryHashesBroken {
        return "high_cardinality_query_pattern"
    }
    
    if _, exists := ct.queryHashes[queryHash]; exists {
        return queryHash
    }
    
    if len(ct.queryHashes) >= ct.limits.MaxQueryHashes {
        ct.queryHashesBroken = true
        ct.logger.Warn("Query hash cardinality limit exceeded - circuit breaker activated",
            zap.Int("limit", ct.limits.MaxQueryHashes),
            zap.Int("current", len(ct.queryHashes)),
        )
        return "high_cardinality_query_pattern"
    }
    
    ct.queryHashes[queryHash] = struct{}{}
    return queryHash
}

// getStats returns current cardinality statistics
func (ct *cardinalityTracker) getStats() map[string]int {
    ct.mu.RLock()
    defer ct.mu.RUnlock()
    
    return map[string]int{
        "unique_users":     len(ct.users),
        "unique_schemas":   len(ct.schemas),
        "unique_databases": len(ct.databases),
        "unique_query_hashes": len(ct.queryHashes),
        "unique_roles":     len(ct.roles),
        "unique_error_codes": len(ct.errorCodes),
    }
}

// getDroppedCounts returns how many values were dropped due to circuit breakers
func (ct *cardinalityTracker) getDroppedCounts() map[string]int64 {
    ct.mu.RLock()
    defer ct.mu.RUnlock()
    
    return map[string]int64{
        "dropped_users":   ct.totalDroppedUsers,
        "dropped_schemas": ct.totalDroppedSchemas,
        "dropped_databases": ct.totalDroppedDbs,
    }
}
