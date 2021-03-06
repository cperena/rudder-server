package bigquery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	warehouseUploadsTable      string
	maxParallelLoads           int
	partitionExpiryUpdated     map[string]bool
	partitionExpiryUpdatedLock sync.RWMutex
	pkgLogger                  logger.LoggerI
)

type HandleT struct {
	BQContext     context.Context
	DbHandle      *sql.DB
	Db            *bigquery.Client
	Namespace     string
	CurrentSchema map[string]map[string]string
	Warehouse     warehouseutils.WarehouseT
	ProjectID     string
	Upload        warehouseutils.UploadT
}

// String constants for bigquery destination config
const (
	GCPProjectID   = "project"
	GCPCredentials = "credentials"
	GCPLocation    = "location"
)

// maps datatype stored in rudder to datatype in bigquery
var dataTypesMap = map[string]bigquery.FieldType{
	"boolean":  bigquery.BooleanFieldType,
	"int":      bigquery.IntegerFieldType,
	"float":    bigquery.FloatFieldType,
	"string":   bigquery.StringFieldType,
	"datetime": bigquery.TimestampFieldType,
}

// maps datatype in bigquery to datatype stored in rudder
var dataTypesMapToRudder = map[bigquery.FieldType]string{
	"BOOLEAN":   "boolean",
	"BOOL":      "boolean",
	"INTEGER":   "int",
	"INT64":     "int",
	"NUMERIC":   "float",
	"FLOAT":     "float",
	"FLOAT64":   "float",
	"STRING":    "string",
	"BYTES":     "string",
	"DATE":      "datetime",
	"DATETIME":  "datetime",
	"TIME":      "datetime",
	"TIMESTAMP": "datetime",
}

var partitionKeyMap = map[string]string{
	"users":                      "id",
	"identifies":                 "id",
	warehouseutils.DiscardsTable: "row_id, column_name, table_name",
}

func (bq *HandleT) setUploadError(err error, state string) {
	warehouseutils.SetUploadStatus(bq.Upload, warehouseutils.ExportingDataFailedState, bq.DbHandle)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=$4`, warehouseUploadsTable)
	_, err = bq.DbHandle.Exec(sqlStatement, state, err.Error(), time.Now(), bq.Upload.ID)
	if err != nil {
		panic(err)
	}
}

func getTableSchema(columns map[string]string) []*bigquery.FieldSchema {
	var schema []*bigquery.FieldSchema
	for columnName, columnType := range columns {
		schema = append(schema, &bigquery.FieldSchema{Name: columnName, Type: dataTypesMap[columnType]})
	}
	return schema
}

func (bq *HandleT) createTable(name string, columns map[string]string) (err error) {
	pkgLogger.Infof("BQ: Creating table: %s in bigquery dataset: %s in project: %s", name, bq.Namespace, bq.ProjectID)
	sampleSchema := getTableSchema(columns)
	metaData := &bigquery.TableMetadata{
		Schema:           sampleSchema,
		TimePartitioning: &bigquery.TimePartitioning{},
	}
	tableRef := bq.Db.Dataset(bq.Namespace).Table(name)
	err = tableRef.Create(bq.BQContext, metaData)
	if !checkAndIgnoreAlreadyExistError(err) {
		return err
	}

	partitionKey := "id"
	if column, ok := partitionKeyMap[name]; ok {
		partitionKey = column
	}

	var viewOrderByStmt string
	if _, ok := columns["loaded_at"]; ok {
		viewOrderByStmt = " ORDER BY loaded_at DESC "
	}

	// assuming it has field named id upon which dedup is done in view
	viewQuery := `SELECT * EXCEPT (__row_number) FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY ` + partitionKey + viewOrderByStmt + `) AS __row_number FROM ` + "`" + bq.ProjectID + "." + bq.Namespace + "." + name + "`" + ` WHERE _PARTITIONTIME BETWEEN TIMESTAMP_TRUNC(TIMESTAMP_MICROS(UNIX_MICROS(CURRENT_TIMESTAMP()) - 60 * 60 * 60 * 24 * 1000000), DAY, 'UTC')
					AND TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'UTC')
			)
		WHERE __row_number = 1`
	metaData = &bigquery.TableMetadata{
		ViewQuery: viewQuery,
	}
	tableRef = bq.Db.Dataset(bq.Namespace).Table(name + "_view")
	err = tableRef.Create(bq.BQContext, metaData)
	return
}

func (bq *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	pkgLogger.Infof("BQ: Adding columns in table %s in bigquery dataset: %s in project: %s", tableName, bq.Namespace, bq.ProjectID)
	tableRef := bq.Db.Dataset(bq.Namespace).Table(tableName)
	meta, err := tableRef.Metadata(bq.BQContext)
	if err != nil {
		return err
	}
	newSchema := append(meta.Schema,
		&bigquery.FieldSchema{Name: columnName, Type: dataTypesMap[columnType]},
	)
	update := bigquery.TableMetadataToUpdate{
		Schema: newSchema,
	}
	_, err = tableRef.Update(bq.BQContext, update, meta.ETag)
	return
}

func (bq *HandleT) createSchema() (err error) {
	pkgLogger.Infof("BQ: Creating bigquery dataset: %s in project: %s", bq.Namespace, bq.ProjectID)
	location := strings.TrimSpace(warehouseutils.GetConfigValue(GCPLocation, bq.Warehouse))
	if location == "" {
		location = "US"
	}
	ds := bq.Db.Dataset(bq.Namespace)
	meta := &bigquery.DatasetMetadata{
		Location: location,
	}
	err = ds.Create(bq.BQContext, meta)
	return
}

func checkAndIgnoreAlreadyExistError(err error) bool {
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == 409 || e.Code == 400 {
				pkgLogger.Debugf("BQ: Google API returned error with code: %v", e.Code)
				return true
			}
		}
		return false
	}
	return true
}

// FetchSchema queries bigquery and returns the schema assoiciated with provided namespace
func (bq *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT, namespace string) (schema map[string]map[string]string, err error) {
	schema = make(map[string]map[string]string)
	bq.Warehouse = warehouse
	bq.ProjectID = strings.TrimSpace(warehouseutils.GetConfigValue(GCPProjectID, bq.Warehouse))
	dbClient, err := bq.connect(BQCredentialsT{
		projectID:   bq.ProjectID,
		credentials: warehouseutils.GetConfigValue(GCPCredentials, bq.Warehouse),
		// location:    warehouseutils.GetConfigValue(GCPLocation, bq.Warehouse),
	})
	if err != nil {
		return
	}
	defer dbClient.Close()

	query := dbClient.Query(fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type
							 FROM %[1]s.INFORMATION_SCHEMA.TABLES as t JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c
							 ON (t.table_name = c.table_name) and (t.table_type != 'VIEW') and c.column_name != '_PARTITIONTIME'`, namespace))

	it, err := query.Read(bq.BQContext)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			// if dataset resource is not found, return empty schema
			if e.Code == 404 {
				return schema, nil
			}
			pkgLogger.Errorf("BQ: Error in fetching schema from bigquery destination:%v, query: %v", bq.Warehouse.Destination.ID, query)
			return schema, e
		}
		pkgLogger.Errorf("BQ: Error in fetching schema from bigquery destination:%v, query: %v", bq.Warehouse.Destination.ID, query)
		return
	}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			pkgLogger.Errorf("BQ: Error in processing fetched schema from redshift destination:%v, error: %v", bq.Warehouse.Destination.ID, err)
			return nil, err
		}
		var tName, cName, cType string
		tName, _ = values[0].(string)
		if _, ok := schema[tName]; !ok {
			schema[tName] = make(map[string]string)
		}
		cName, _ = values[1].(string)
		cType, _ = values[2].(string)
		if datatype, ok := dataTypesMapToRudder[bigquery.FieldType(cType)]; ok {
			// lower case all column names from bigquery
			schema[tName][strings.ToLower(cName)] = datatype
		}
	}
	return
}

func (bq *HandleT) updateSchema() (updatedSchema map[string]map[string]string, err error) {
	diff := warehouseutils.GetSchemaDiff(bq.CurrentSchema, bq.Upload.Schema)
	updatedSchema = diff.UpdatedSchema
	if len(bq.CurrentSchema) == 0 {
		err = bq.createSchema()
		if !checkAndIgnoreAlreadyExistError(err) {
			return nil, err
		}
	}
	processedTables := make(map[string]bool)
	for _, tableName := range diff.Tables {
		err = bq.createTable(tableName, diff.ColumnMaps[tableName])
		if !checkAndIgnoreAlreadyExistError(err) {
			return nil, err
		}
		processedTables[tableName] = true
	}
	for tableName, columnMap := range diff.ColumnMaps {
		if _, ok := processedTables[tableName]; ok {
			continue
		}
		if len(columnMap) > 0 {
			var err error
			for columnName, columnType := range columnMap {
				err = bq.addColumn(tableName, columnName, columnType)
				if err != nil {
					if checkAndIgnoreAlreadyExistError(err) {
						pkgLogger.Infof("BQ: Column %s already exists on %s.%s \nResponse: %v", columnName, bq.Namespace, tableName, err)
					} else {
						return nil, err
					}
				}
			}
		}
	}
	return updatedSchema, nil
}

func partitionedTable(tableName string, partitionDate string) string {
	return fmt.Sprintf(`%s$%v`, tableName, strings.ReplaceAll(partitionDate, "-", ""))
}

func (bq *HandleT) loadTable(tableName string, forceLoad bool) (partitionDate string, err error) {
	if !forceLoad {
		uploadStatus, err := warehouseutils.GetTableUploadStatus(bq.Upload.ID, tableName, bq.DbHandle)
		if err != nil {
			panic(err)
		}

		if uploadStatus == warehouseutils.ExportedDataState {
			pkgLogger.Infof("BQ: Skipping load for table:%s as it has been succesfully loaded earlier", tableName)
			return "", nil
		}
	}

	if !warehouseutils.HasLoadFiles(bq.DbHandle, bq.Warehouse.Source.ID, bq.Warehouse.Destination.ID, tableName, bq.Upload.StartLoadFileID, bq.Upload.EndLoadFileID) {
		warehouseutils.SetTableUploadStatus(warehouseutils.ExportedDataState, bq.Upload.ID, tableName, bq.DbHandle)
		return
	}

	pkgLogger.Infof("BQ: Starting load for table:%s\n", tableName)
	warehouseutils.SetTableUploadStatus(warehouseutils.ExecutingState, bq.Upload.ID, tableName, bq.DbHandle)

	locations, err := warehouseutils.GetLoadFileLocations(bq.DbHandle, bq.Warehouse.Source.ID, bq.Warehouse.Destination.ID, tableName, bq.Upload.StartLoadFileID, bq.Upload.EndLoadFileID)
	if err != nil {
		panic(err)
	}
	locations = warehouseutils.GetGCSLocations(locations, warehouseutils.GCSLocationOptionsT{})
	pkgLogger.Infof("BQ: Loading data into table: %s in bigquery dataset: %s in project: %s from %v", tableName, bq.Namespace, bq.ProjectID, locations)
	gcsRef := bigquery.NewGCSReference(locations...)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.MaxBadRecords = 0
	gcsRef.IgnoreUnknownValues = false

	partitionDate = time.Now().Format("2006-01-02")
	outputTable := partitionedTable(tableName, partitionDate)

	// create partitioned table in format tableName$20191221
	loader := bq.Db.Dataset(bq.Namespace).Table(outputTable).LoaderFrom(gcsRef)

	job, err := loader.Run(bq.BQContext)
	if err != nil {
		pkgLogger.Errorf("BQ: Error initiating load job: %v\n", err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, bq.Upload.ID, tableName, err, bq.DbHandle)
		return
	}
	status, err := job.Wait(bq.BQContext)
	if err != nil {
		pkgLogger.Errorf("BQ: Error running load job: %v\n", err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, bq.Upload.ID, tableName, err, bq.DbHandle)
		return
	}

	if status.Err() != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, bq.Upload.ID, tableName, status.Err(), bq.DbHandle)
		return "", status.Err()
	}
	warehouseutils.SetTableUploadStatus(warehouseutils.ExportedDataState, bq.Upload.ID, tableName, bq.DbHandle)
	return
}

func (bq *HandleT) loadUserTables() (err error) {
	pkgLogger.Infof("BQ: Starting load for identifies and users tables\n")
	partitionDate, err := bq.loadTable(warehouseutils.IdentifiesTable, true)
	if err != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, bq.Upload.ID, warehouseutils.IdentifiesTable, err, bq.DbHandle)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, bq.Upload.ID, warehouseutils.UsersTable, errors.New("Failed to upload identifies table"), bq.DbHandle)
		return
	}

	if _, ok := bq.Upload.Schema["users"]; !ok {
		return
	}

	firstValueSQL := func(column string) string {
		return fmt.Sprintf(`FIRST_VALUE(%[1]s IGNORE NULLS) OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS %[1]s`, column)
	}

	loadedAtFilter := func() string {
		// get first event received_at time in this upload for identifies table
		// firstEventAt := func() string {
		// 	return warehouseutils.GetTableFirstEventAt(bq.DbHandle, bq.Warehouse.Source.ID, bq.Warehouse.Destination.ID, warehouseutils.IdentifiesTable, bq.Upload.StartLoadFileID, bq.Upload.EndLoadFileID)
		// }

		// TODO: Add this filter to optimize reading from identifies table since first event in upload
		// rather than entire day's records
		// commented it since firstEventAt is not stored in UTC format in earlier versions
		// return fmt.Sprintf(`AND loaded_at >= TIMESTAMP('%s')`, firstEventAt())
		return ""
	}

	userColMap := bq.CurrentSchema["users"]
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, colName)
		firstValProps = append(firstValProps, firstValueSQL(colName))
	}

	bqTable := func(name string) string { return fmt.Sprintf("`%s`.`%s`", bq.Namespace, name) }

	bqUsersTable := bqTable(warehouseutils.UsersTable)
	bqIdentifiesTable := bqTable(warehouseutils.IdentifiesTable)
	partition := fmt.Sprintf("TIMESTAMP('%s')", partitionDate)
	identifiesFrom := fmt.Sprintf(`%s WHERE _PARTITIONTIME = %s AND user_id IS NOT NULL %s`, bqIdentifiesTable, partition, loadedAtFilter())
	sqlStatement := fmt.Sprintf(`SELECT DISTINCT * FROM (
			SELECT id, %[1]s FROM (
				(
					SELECT id, %[2]s FROM %[3]s WHERE (
						id in (SELECT user_id FROM %[4]s)
					)
				) UNION ALL (
					SELECT user_id, %[2]s FROM %[4]s
				)
			)
		)`,
		strings.Join(firstValProps, ","), // 1
		strings.Join(userColNames, ","),  // 2
		bqUsersTable,                     // 3
		identifiesFrom,                   // 4
	)

	pkgLogger.Infof(`BQ: Loading data into users table: %v`, sqlStatement)
	partitionedUsersTable := partitionedTable(warehouseutils.UsersTable, partitionDate)
	query := bq.Db.Query(sqlStatement)
	query.QueryConfig.Dst = bq.Db.Dataset(bq.Namespace).Table(partitionedUsersTable)
	query.WriteDisposition = bigquery.WriteAppend

	tableName := "users"
	job, err := query.Run(bq.BQContext)
	if err != nil {
		pkgLogger.Errorf("BQ: Error initiating load job: %v\n", err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, bq.Upload.ID, tableName, err, bq.DbHandle)
		return
	}
	status, err := job.Wait(bq.BQContext)
	if err != nil {
		pkgLogger.Errorf("BQ: Error running load job: %v\n", err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, bq.Upload.ID, tableName, err, bq.DbHandle)
		return
	}

	if status.Err() != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, bq.Upload.ID, tableName, err, bq.DbHandle)
		return
	}

	return
}

func (bq *HandleT) load() (errList []error) {
	pkgLogger.Infof("BQ: Starting load for all %v tables\n", len(bq.Upload.Schema))
	if _, ok := bq.Upload.Schema["identifies"]; ok {
		err := bq.loadUserTables()
		if err != nil {
			errList = append(errList, err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(len(bq.Upload.Schema))
	loadChan := make(chan struct{}, maxParallelLoads)
	for tableName := range bq.Upload.Schema {
		if tableName == "users" || tableName == "identifies" {
			wg.Done()
			continue
		}

		tName := tableName
		loadChan <- struct{}{}
		rruntime.Go(func() {
			_, err := bq.loadTable(tName, false)
			if err != nil {
				errList = append(errList, err)
			}
			wg.Done()
			<-loadChan
		})
	}
	wg.Wait()
	pkgLogger.Infof("BQ: Completed load for all tables\n")
	return
}

type BQCredentialsT struct {
	projectID   string
	credentials string
	location    string
}

func (bq *HandleT) connect(cred BQCredentialsT) (*bigquery.Client, error) {
	pkgLogger.Infof("BQ: Connecting to BigQuery in project: %s", cred.projectID)
	bq.BQContext = context.Background()
	client, err := bigquery.NewClient(bq.BQContext, cred.projectID, option.WithCredentialsJSON([]byte(cred.credentials)))
	return client, err
	// if err != nil {
	// 	return nil, err
	// }

	// location := strings.TrimSpace(cred.location)
	// if location != "" {
	// 	client.Location = location
	// }

	// return client, nil
}

func loadConfig() {
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	maxParallelLoads = config.GetInt("Warehouse.bigquery.maxParallelLoads", 20)
	partitionExpiryUpdated = make(map[string]bool)
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("bigquery")

}

func (bq *HandleT) MigrateSchema() (err error) {
	timer := warehouseutils.DestStat(stats.TimerType, "migrate_schema_time", bq.Warehouse.Destination.ID)
	timer.Start()
	warehouseutils.SetUploadStatus(bq.Upload, warehouseutils.UpdatingSchemaState, bq.DbHandle)
	pkgLogger.Infof("BQ: Updating schema for bigquery in project: %s", bq.ProjectID)
	updatedSchema, err := bq.updateSchema()
	if err != nil {
		warehouseutils.SetUploadError(bq.Upload, err, warehouseutils.UpdatingSchemaFailedState, bq.DbHandle)
		return
	}
	bq.CurrentSchema = updatedSchema
	err = warehouseutils.SetUploadStatus(bq.Upload, warehouseutils.UpdatedSchemaState, bq.DbHandle)
	if err != nil {
		panic(err)
	}
	err = warehouseutils.UpdateCurrentSchema(bq.Namespace, bq.Warehouse, bq.Upload.ID, updatedSchema, bq.DbHandle)
	timer.End()
	if err != nil {
		warehouseutils.SetUploadError(bq.Upload, err, warehouseutils.UpdatingSchemaFailedState, bq.DbHandle)
		return
	}
	return
}

func (bq *HandleT) Export() (err error) {
	pkgLogger.Infof("BQ: Starting export to Bigquery for source:%s and wh_upload:%v", bq.Warehouse.Source.ID, bq.Upload.ID)
	err = warehouseutils.SetUploadStatus(bq.Upload, warehouseutils.ExportingDataState, bq.DbHandle)
	if err != nil {
		panic(err)
	}
	timer := warehouseutils.DestStat(stats.TimerType, "upload_time", bq.Warehouse.Destination.ID)
	timer.Start()
	errList := bq.load()
	timer.End()
	if len(errList) > 0 {
		errStr := ""
		for idx, err := range errList {
			errStr += err.Error()
			if idx < len(errList)-1 {
				errStr += ", "
			}
		}
		warehouseutils.SetUploadError(bq.Upload, errors.New(errStr), warehouseutils.ExportingDataFailedState, bq.DbHandle)
		return errors.New(errStr)
	}
	err = warehouseutils.SetUploadStatus(bq.Upload, warehouseutils.ExportedDataState, bq.DbHandle)
	if err != nil {
		panic(err)
	}
	return
}

func (bq *HandleT) removePartitionExpiry() (err error) {
	partitionExpiryUpdatedLock.Lock()
	defer partitionExpiryUpdatedLock.Unlock()
	identifier := fmt.Sprintf(`%s::%s`, bq.Upload.SourceID, bq.Upload.DestinationID)
	if _, ok := partitionExpiryUpdated[identifier]; ok {
		return
	}
	for tName := range bq.CurrentSchema {
		var m *bigquery.TableMetadata
		m, err = bq.Db.Dataset(bq.Namespace).Table(tName).Metadata(bq.BQContext)
		if err != nil {
			return
		}
		if m.TimePartitioning != nil && m.TimePartitioning.Expiration > 0 {
			_, err = bq.Db.Dataset(bq.Namespace).Table(tName).Update(bq.BQContext, bigquery.TableMetadataToUpdate{TimePartitioning: &bigquery.TimePartitioning{Expiration: time.Duration(0)}}, "")
			if err != nil {
				return
			}
		}
	}
	partitionExpiryUpdated[identifier] = true
	return
}

func (bq *HandleT) CrashRecover(config warehouseutils.ConfigT) (err error) {
	return
}

func (bq *HandleT) Process(config warehouseutils.ConfigT) (err error) {
	bq.DbHandle = config.DbHandle
	bq.Warehouse = config.Warehouse
	bq.Upload = config.Upload
	bq.ProjectID = strings.TrimSpace(warehouseutils.GetConfigValue(GCPProjectID, bq.Warehouse))

	bq.Db, err = bq.connect(BQCredentialsT{
		projectID:   bq.ProjectID,
		credentials: warehouseutils.GetConfigValue(GCPCredentials, bq.Warehouse),
		// location:    warehouseutils.GetConfigValue(GCPLocation, bq.Warehouse),
	})
	if err != nil {
		warehouseutils.SetUploadError(bq.Upload, err, warehouseutils.UpdatingSchemaFailedState, bq.DbHandle)
		return err
	}
	defer bq.Db.Close()
	currSchema, err := warehouseutils.GetCurrentSchema(bq.DbHandle, bq.Warehouse)
	if err != nil {
		panic(err)
	}
	bq.CurrentSchema = currSchema
	bq.Namespace = bq.Upload.Namespace

	// done here to have access to latest schema in warehouse
	err = bq.removePartitionExpiry()
	if err != nil {
		pkgLogger.Errorf("BQ: Removing Expiration on partition failed: %v", err)
		warehouseutils.SetUploadError(bq.Upload, err, warehouseutils.UpdatingSchemaFailedState, bq.DbHandle)
		return err
	}

	if config.Stage == "ExportData" {
		err = bq.Export()
	} else {
		err = bq.MigrateSchema()
		if err == nil {
			err = bq.Export()
		}
	}
	return
}

func (bq *HandleT) TestConnection(config warehouseutils.ConfigT) (err error) {
	bq.Warehouse = config.Warehouse
	bq.Db, err = bq.connect(BQCredentialsT{
		projectID:   bq.ProjectID,
		credentials: warehouseutils.GetConfigValue(GCPCredentials, bq.Warehouse),
	})
	if err != nil {
		return
	}
	defer bq.Db.Close()
	return
}
