package diff

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/stripe/pg-schema-diff/internal/schema"
	"github.com/stripe/pg-schema-diff/pkg/log"
	"github.com/stripe/pg-schema-diff/pkg/sqldb"
	"github.com/stripe/pg-schema-diff/pkg/tempdb"
)

type schemaSourcePlanDeps struct {
	tempDBFactory tempdb.Factory
	logger        log.Logger
	getSchemaOpts []schema.GetSchemaOpt
}

type SchemaSource interface {
	GetSchema(ctx context.Context, deps schemaSourcePlanDeps) (schema.Schema, error)
}

type (
	ddlStatement struct {
		// stmt is the DDL statement to run.
		stmt string
		// file is an optional field that can be used to store the file name from which the DDL was read.
		file string
	}

	ddlSchemaSource struct {
		ddl []ddlStatement
	}
)

// DirSchemaSource returns a SchemaSource that returns a schema based on the provided directories. You must provide a tempDBFactory
// via the WithTempDbFactory option.
func DirSchemaSource(dir string, logger log.Logger) (SchemaSource, error) {
	logger.Debugf("Reading DDL from directory: %s", dir)
	stmts, err := getDDLFromPath(dir, logger)
	if err != nil {
		return &ddlSchemaSource{}, err
	}
	return &ddlSchemaSource{
		ddl: stmts,
	}, nil
}

// FilesSchemaSource returns a SchemaSource that returns a schema based on the provided files. You must provide a tempDBFactory
// via the WithTempDbFactory option.
func FilesSchemaSource(files []string, logger log.Logger) (SchemaSource, error) {
	logger.Debugf("Reading DDL from files: %s", strings.Join(files, ", "))
	var ddl []ddlStatement
	for _, file := range files {
		fileContents, err := os.ReadFile(file)
		if err != nil {
			logger.Errorf("Error reading file %q: %v", file, err)
			return nil, fmt.Errorf("reading file %q: %w", file, err)
		}
		lines := strings.Split(string(fileContents), "\n")
		logger.Debugf("Successfully read SQL file: %s (%d lines)", file, len(lines))
		ddl = append(ddl, ddlStatement{
			stmt: string(fileContents),
			file: file,
		})
	}
	return &ddlSchemaSource{
		ddl: ddl,
	}, nil
}

// getDDLFromPath reads all .sql files under the given path (including sub-directories) and returns the DDL
// in lexical order.
func getDDLFromPath(path string, logger log.Logger) ([]ddlStatement, error) {
	var filePaths []string
	if err := filepath.Walk(path, func(path string, entry os.FileInfo, err error) error {
		if err != nil {
			logger.Errorf("Error walking path %q: %v", path, err)
			return fmt.Errorf("walking path %q: %w", path, err)
		}
		if entry.IsDir() {
			logger.Debugf("Skipping directory: %s", path)
			return nil
		}
		if strings.ToLower(filepath.Ext(entry.Name())) != ".sql" {
			logger.Debugf("Skipping non-SQL file: %s", path)
			return nil
		}
		filePaths = append(filePaths, path)
		return nil
	}); err != nil {
		return nil, err
	}

	// Sort the file paths to ensure a consistent order of execution.
	sort.Strings(filePaths)

	var ddl []ddlStatement
	for _, path := range filePaths {
		fileContents, err := os.ReadFile(path)
		if err != nil {
			logger.Errorf("Error reading file %q: %v", path, err)
			return nil, fmt.Errorf("reading file %q: %w", path, err)
		}
		lines := strings.Split(string(fileContents), "\n")
		logger.Debugf("Successfully read SQL file: %s (%d lines)", path, len(lines))
		// In the future, it would make sense to split the file contents into individual DDL statements; however,
		// that would require fully parsing the SQL. Naively splitting on `;` would not work because `;` can be
		// used in comments, strings, and escaped identifiers.
		ddl = append(ddl, ddlStatement{
			stmt: string(fileContents),
			file: path,
		})
	}
	return ddl, nil
}


// DDLSchemaSource returns a SchemaSource that returns a schema based on the provided DDL. You must provide a tempDBFactory
// via the WithTempDbFactory option.
func DDLSchemaSource(stmts []string, logger log.Logger) SchemaSource {
	var ddl []ddlStatement
	for _, stmt := range stmts {
		ddl = append(ddl, ddlStatement{
			stmt: stmt,
			// There is no file name associated with the DDL statement.
			file: "",
		})
	}
	logger.Debugf("DDLSchemaSource created with %d statements", len(stmts))
	return &ddlSchemaSource{ddl: ddl}
}

func (s *ddlSchemaSource) GetSchema(ctx context.Context, deps schemaSourcePlanDeps) (schema.Schema, error) {
	if deps.tempDBFactory == nil {
		return schema.Schema{}, fmt.Errorf("a temp db factory is required to get the schema from DDL")
	}

	tempDb, err := deps.tempDBFactory.Create(ctx)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("creating temp database: %w", err)
	}
	defer func(closer tempdb.ContextualCloser) {
		if err := closer.Close(ctx); err != nil {
			deps.logger.Errorf("an error occurred while dropping the temp database: %s", err)
		}
	}(tempDb.ContextualCloser)

	for _, ddlStmt := range s.ddl {
		if _, err := tempDb.ConnPool.ExecContext(ctx, ddlStmt.stmt); err != nil {
			debugInfo := ""
			if ddlStmt.file != "" {
				debugInfo = fmt.Sprintf(" (from %s)", ddlStmt.file)
			}
			return schema.Schema{}, fmt.Errorf("running DDL%s: %w", debugInfo, err)
		}
	}

	return schema.GetSchema(ctx, tempDb.ConnPool, append(deps.getSchemaOpts, tempDb.ExcludeMetadataOptions...)...)
}

type dbSchemaSource struct {
	queryable sqldb.Queryable
}

// DBSchemaSource returns a SchemaSource that returns a schema based on the provided queryable. It is recommended
// that the sqldb.Queryable is a *sql.DB with a max # of connections set.
func DBSchemaSource(queryable sqldb.Queryable) SchemaSource {
	return &dbSchemaSource{queryable: queryable}
}

func (s *dbSchemaSource) GetSchema(ctx context.Context, deps schemaSourcePlanDeps) (schema.Schema, error) {
	return schema.GetSchema(ctx, s.queryable, deps.getSchemaOpts...)
}
