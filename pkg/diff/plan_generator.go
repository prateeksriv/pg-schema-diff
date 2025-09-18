package diff

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/kr/pretty"
	"github.com/stripe/pg-schema-diff/internal/schema"
	externalschema "github.com/stripe/pg-schema-diff/pkg/schema"

	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

const (
	tempDbMaxConnections = 5
)

var (
	errTempDbFactoryRequired = fmt.Errorf("tempDbFactory is required. include the option WithTempDbFactory")
)

type (
	planOptions struct {
		tempDbFactory           tempdb.Factory
		dataPackNewTables       bool
		ignoreChangesToColOrder bool
		logger                  log.Logger
		validatePlan            bool
		getSchemaOpts           []schema.GetSchemaOpt
		randReader              io.Reader
		noConcurrentIndexOps    bool
		outputFilePath          string
	}

	PlanOpt func(opts *planOptions)
)

func WithTempDbFactory(factory tempdb.Factory) PlanOpt {
	return func(opts *planOptions) {
		opts.tempDbFactory = factory
	}
}

// WithOutputFile configures the plan generation to write the plan and statements to the specified file path.
// If not provided, temporary files will be used.
func WithOutputFile(filePath string) PlanOpt {
	return func(opts *planOptions) {
		opts.outputFilePath = filePath
	}
}

// WithDataPackNewTables configures the plan generation such that it packs the columns in the new tables to minimize
// padding. It will help minimize the storage used by the tables
func WithDataPackNewTables() PlanOpt {
	return func(opts *planOptions) {
		opts.dataPackNewTables = true
	}
}

// WithRespectColumnOrder configures the plan generation to respect any changes to the ordering of columns in
// existing tables. You will most likely want this disabled, since column ordering changes are common
func WithRespectColumnOrder() PlanOpt {
	return func(opts *planOptions) {
		opts.ignoreChangesToColOrder = false
	}
}

// WithDoNotValidatePlan disables plan validation, where the migration plan is tested against a temporary database
// instance.
func WithDoNotValidatePlan() PlanOpt {
	return func(opts *planOptions) {
		opts.validatePlan = false
	}
}

// WithLogger configures plan generation to use the provided logger instead of the default
func WithLogger(logger log.Logger) PlanOpt {
	return func(opts *planOptions) {
		opts.logger = logger
	}
}

func WithIncludeSchemas(schemas ...string) PlanOpt {
	return func(opts *planOptions) {
		opts.getSchemaOpts = append(opts.getSchemaOpts, schema.WithIncludeSchemas(schemas...))
	}
}

func WithExcludeSchemas(schemas ...string) PlanOpt {
	return func(opts *planOptions) {
		opts.getSchemaOpts = append(opts.getSchemaOpts, schema.WithExcludeSchemas(schemas...))
	}
}

func WithGetSchemaOpts(getSchemaOpts ...externalschema.GetSchemaOpt) PlanOpt {
	return func(opts *planOptions) {
		opts.getSchemaOpts = append(opts.getSchemaOpts, getSchemaOpts...)
	}
}

// WithRandReader seeds the random used to generate random SQL identifiers, e.g., temporary not-null check constraints.
func WithRandReader(randReader io.Reader) PlanOpt {
	return func(opts *planOptions) {
		opts.randReader = randReader
	}
}

// WithNoConcurrentIndexOps disables the use of CONCURRENTLY in CREATE INDEX and DROP INDEX statements.
// This can be useful when you need simpler DDL statements or when working in environments that don't support
// concurrent index operations. Note that disabling concurrent operations may result in longer lock times
// and potential downtime during migrations.
func WithNoConcurrentIndexOps() PlanOpt {
	return func(opts *planOptions) {
		opts.noConcurrentIndexOps = true
	}
}

// Generate generates a migration plan to migrate the database to the target schema
//
// Parameters:
// fromSchema:		The target schema to generate the diff for.
// targetSchema:	The (source of the) schema you want to migrate the database to. Use DDLSchemaSource if the new
// schema is encoded in DDL.
// opts: 			Additional options to configure the plan generation
func Generate(
	ctx context.Context,
	fromSchema SchemaSource,
	targetSchema SchemaSource,
	opts ...PlanOpt,
) (Plan, error) {
	planOptions := &planOptions{
		validatePlan:            true,
		ignoreChangesToColOrder: true,
		logger:                  log.SimpleLogger(false, false),
		randReader:              rand.Reader,
	}
	for _, opt := range opts {
		opt(planOptions)
	}

	currentSchema, err := fromSchema.GetSchema(ctx, schemaSourcePlanDeps{
		tempDBFactory: planOptions.tempDbFactory,
		logger:        planOptions.logger,
		getSchemaOpts: planOptions.getSchemaOpts,
	})
	if err != nil {
		return Plan{}, fmt.Errorf("getting current schema: %w", err)
	}
	newSchema, err := targetSchema.GetSchema(ctx, schemaSourcePlanDeps{
		tempDBFactory: planOptions.tempDbFactory,
		logger:        planOptions.logger,
		getSchemaOpts: planOptions.getSchemaOpts,
	})
	if err != nil {
		return Plan{}, fmt.Errorf("getting new schema: %w", err)
	}

	statements, err := generateMigrationStatements(currentSchema, newSchema, planOptions)
	if err != nil {
		return Plan{}, fmt.Errorf("generating plan statements: %w", err)
	}

	hash, err := currentSchema.Hash()
	if err != nil {
		return Plan{}, fmt.Errorf("generating current schema hash: %w", err)
	}

	plan := Plan{
		Statements:        statements,
		CurrentSchemaHash: hash,
	}

	planFilePath, statementsFilePath := writePlanAndStatementsToFiles(plan, planOptions.logger, planOptions.outputFilePath)

	if planOptions.validatePlan {
		if planOptions.tempDbFactory == nil {
			return Plan{}, fmt.Errorf("cannot validate plan without a tempDbFactory: %w", errTempDbFactoryRequired)
		}
		if err := assertValidPlan(ctx, planOptions.tempDbFactory, currentSchema, newSchema, plan, planOptions); err != nil {
			return Plan{}, fmt.Errorf("validating migration plan: %w. See logs for detailed plan in %s and statements in %s.", err, planFilePath, statementsFilePath)
		}
	}

	return plan, nil
}

func generateMigrationStatements(oldSchema, newSchema schema.Schema, planOptions *planOptions) ([]Statement, error) {
	diff, _, err := buildSchemaDiff(oldSchema, newSchema)
	if err != nil {
		return nil, err
	}

	if planOptions.dataPackNewTables {
		// Instead of enabling ignoreChangesToColOrder by default, force the user to enable ignoreChangesToColOrder.
		// This ensures the user knows what's going on behind-the-scenes
		if !planOptions.ignoreChangesToColOrder {
			return nil, fmt.Errorf("cannot data pack new tables without also ignoring changes to column order")
		}
		diff = dataPackNewTables(diff)
	}
	if planOptions.ignoreChangesToColOrder {
		diff = removeChangesToColumnOrdering(diff)
	}

	statements, err := newSchemaSQLGenerator(planOptions.randReader, planOptions).Alter(diff)
	if err != nil {
		return nil, fmt.Errorf("generating migration statements: %w", err)
	}
	return statements, nil
}

func assertValidPlan(ctx context.Context,
	tempDbFactory tempdb.Factory,
	currentSchema, newSchema schema.Schema,
	plan Plan,
	planOptions *planOptions,
) error {
	planOptions.logger.Infof("Asserting valid plan...")
	planOptions.logger.Debugf("Current schema: %s", pretty.Sprint(currentSchema))
	planOptions.logger.Debugf("New schema: %s", pretty.Sprint(newSchema))
	planOptions.logger.Debugf("Plan statements: %s", pretty.Sprint(plan.Statements))

	tempDb, err := tempDbFactory.Create(ctx)
	if err != nil {
		planOptions.logger.Debugf("Error creating temporary database: %v", err)
		return err
	}
	defer func(closer tempdb.ContextualCloser) {
		if err := closer.Close(ctx); err != nil {
			planOptions.logger.Errorf("an error occurred while dropping the temp database: %s", err)
		}
	}(tempDb.ContextualCloser)
	// Set a max connections if a user has not set one. This is to prevent us from exploding the number of connections
	// on the database.
	setMaxConnectionsIfNotSet(tempDb.ConnPool, tempDbMaxConnections)

	planOptions.logger.Debugf("Setting schema for empty database...")
	if err := setSchemaForEmptyDatabase(ctx, tempDb, currentSchema, planOptions); err != nil {
		planOptions.logger.Debugf("Error setting schema for empty database: %v", err)
		return fmt.Errorf("inserting schema in temporary database: %w", err)
	}
	planOptions.logger.Debugf("Schema set for empty database.")

	planOptions.logger.Debugf("Executing migration plan statements...")
	if err := executeStatementsIgnoreTimeouts(ctx, tempDb.ConnPool, plan.Statements, planOptions.logger); err != nil {
		planOptions.logger.Debugf("Error executing migration plan statements: %v", err)
		return fmt.Errorf("running migration plan: %w", err)
	}
	planOptions.logger.Debugf("Migration plan statements executed.")

	planOptions.logger.Debugf("Fetching schema from migrated database...")
	migratedSchema, err := schemaFromTempDb(ctx, tempDb, planOptions)
	if err != nil {
		planOptions.logger.Debugf("Error fetching schema from migrated database: %v", err)
		return fmt.Errorf("fetching schema from migrated database: %w", err)
	}
	planOptions.logger.Debugf("Migrated schema: %s", pretty.Sprint(migratedSchema))

	planOptions.logger.Debugf("Asserting migrated schema matches target...")
	return assertMigratedSchemaMatchesTarget(migratedSchema, newSchema, planOptions)
}

func setMaxConnectionsIfNotSet(db *sql.DB, defaultMax int) {
	if db.Stats().MaxOpenConnections <= 0 {
		db.SetMaxOpenConns(defaultMax)
	}
}

func setSchemaForEmptyDatabase(ctx context.Context, emptyDb *tempdb.Database, targetSchema schema.Schema, options *planOptions) error {
	options.logger.Debugf("Target schema for empty database: %s", pretty.Sprint(targetSchema))

	// We can't create invalid indexes. We'll mark them valid in the schema, which should be functionally
	// equivalent for the sake of DDL and other statements.
	//
	// Make a new array, so we don't mutate the underlying array of the original schema. Ideally, we have a clone function
	// in the future
	var validIndexes []schema.Index
	for _, idx := range targetSchema.Indexes {
		idx.IsInvalid = false
		validIndexes = append(validIndexes, idx)
	}
	targetSchema.Indexes = validIndexes

	// An empty database doesn't necessarily have an empty schema, so we should fetch it.
	options.logger.Debugf("Getting starting schema from empty database...")
	startingSchema, err := schemaFromTempDb(ctx, emptyDb, options)
	if err != nil {
		options.logger.Debugf("Error getting starting schema from empty database: %v", err)
		return fmt.Errorf("getting schema from empty database: %w", err)
	}
	options.logger.Debugf("Starting schema from empty database: %s", pretty.Sprint(startingSchema))

	options.logger.Debugf("Generating migration statements for empty database...")
	statements, err := generateMigrationStatements(startingSchema, targetSchema, &planOptions{})
	if err != nil {
		options.logger.Debugf("Error building schema diff for empty database: %v", err)
		return fmt.Errorf("building schema diff: %w", err)
	}
	options.logger.Debugf("Generated statements for empty database: %s", pretty.Sprint(statements))

	options.logger.Debugf("Executing statements for empty database...")
	if err := executeStatementsIgnoreTimeouts(ctx, emptyDb.ConnPool, statements, options.logger); err != nil {
		options.logger.Debugf("Error executing statements for empty database: %v", err)
		return fmt.Errorf("executing statements: %w\n%# v", err, pretty.Formatter(statements))
	}
	options.logger.Debugf("Statements executed for empty database.")
	return nil
}

func schemaFromTempDb(ctx context.Context, db *tempdb.Database, plan *planOptions) (schema.Schema, error) {
	return schema.GetSchema(ctx, db.ConnPool, append(plan.getSchemaOpts, db.ExcludeMetadataOptions...)...)
}

func assertMigratedSchemaMatchesTarget(migratedSchema, targetSchema schema.Schema, planOptions *planOptions) error {
	planOptions.logger.Debugf("Comparing migrated schema to target schema...")
	planOptions.logger.Debugf("Migrated schema: %s", pretty.Sprint(migratedSchema))
	planOptions.logger.Debugf("Target schema: %s", pretty.Sprint(targetSchema))

	toTargetSchemaStmts, err := generateMigrationStatements(migratedSchema, targetSchema, planOptions)
	if err != nil {
		planOptions.logger.Debugf("Error building schema diff between migrated database and new schema: %v", err)
		return fmt.Errorf("building schema diff between migrated database and new schema: %w", err)
	}

	if len(toTargetSchemaStmts) > 0 {
		var stmtsStrs []string
		for _, stmt := range toTargetSchemaStmts {
			stmtsStrs = append(stmtsStrs, stmt.DDL)
		}
		planOptions.logger.Errorf("Unexpected statements when migrating from migrated schema to target schema:\n%s", strings.Join(stmtsStrs, "\n"))
		return fmt.Errorf("validating plan failed. diff detected. See logs for detailed statements.")
	}
	planOptions.logger.Debugf("No unexpected statements found. Migrated schema matches target.")

	return nil
}

// executeStatementsIgnoreTimeouts executes the statements using the sql connection but ignores any provided timeouts.
// This function is currently used to validate migration plans.
func executeStatementsIgnoreTimeouts(ctx context.Context, connPool *sql.DB, statements []Statement, logger log.Logger) error {
	conn, err := connPool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("getting connection from pool: %w", err)
	}
	defer conn.Close()

	// Set a session-level statement_timeout to bound the execution of the migration plan.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION statement_timeout = %d", (10*time.Second).Milliseconds())); err != nil {
		return fmt.Errorf("setting statement timeout: %w", err)
	}
	// Due to the way *sql.Db works, when a statement_timeout is set for the session, it will NOT reset
	// by default when it's returned to the pool.
	//
	// We can't set the timeout at the TRANSACTION-level (for each transaction) because `ADD INDEX CONCURRENTLY`
	// must be executed within its own transaction block. Postgres will error if you try to set a TRANSACTION-level
	// timeout for it. SESSION-level statement_timeouts are respected by `ADD INDEX CONCURRENTLY`
	for _, stmt := range statements {
		logger.Debugf("Executing statement: %s", stmt.ToSQL())
		if _, err := conn.ExecContext(ctx, stmt.ToSQL()); err != nil {
			logger.Debugf("Error executing statement %s: %v", stmt.ToSQL(), err)
			return fmt.Errorf("executing migration statement: %s: %w", stmt, err)
		}
	}
	return nil
}

func writePlanAndStatementsToFiles(plan Plan, logger log.Logger, outputFilePath string) (planFilePath string, statementsFilePath string) {
	var planFile *os.File
	var statementsFile *os.File
	var err error

	if outputFilePath != "" {
		planFilePath = outputFilePath + "_plan.txt"
		statementsFilePath = outputFilePath + "_statements.sql"

		planFile, err = os.Create(planFilePath)
		if err != nil {
			logger.Errorf("Error creating plan file at %s: %v", planFilePath, err)
			return "", ""
		}
		statementsFile, err = os.Create(statementsFilePath)
		if err != nil {
			logger.Errorf("Error creating statements file at %s: %v", statementsFilePath, err)
			planFile.Close()
			return planFilePath, ""
		}
	} else {
		planFile, err = os.CreateTemp("", "plan-*.txt")
		if err != nil {
			logger.Errorf("Error creating temporary file for plan: %v", err)
			return "", ""
		}
		planFilePath = planFile.Name()

		statementsFile, err = os.CreateTemp("", "statements-*.sql")
		if err != nil {
			logger.Errorf("Error creating temporary file for statements: %v", err)
			planFile.Close()
			return planFilePath, ""
		}
		statementsFilePath = statementsFile.Name()
	}

	defer planFile.Close()
	defer statementsFile.Close()

	if _, err := planFile.WriteString(pretty.Sprint(plan)); err != nil {
		logger.Errorf("Error writing plan to file: %v", err)
		return planFilePath, statementsFilePath
	}
	logger.Infof("Detailed plan written to: %s", planFilePath)

	var sb strings.Builder
	for _, stmt := range plan.Statements {
		sb.WriteString(stmt.ToSQL())
		sb.WriteString("\n")
	}
	if _, err := statementsFile.WriteString(sb.String()); err != nil {
		logger.Errorf("Error writing statements to file: %v", err)
		return planFilePath, statementsFilePath
	}
	logger.Infof("Detailed statements written to: %s", statementsFilePath)

	return planFilePath, statementsFilePath
}