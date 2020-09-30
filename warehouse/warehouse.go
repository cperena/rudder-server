package warehouse

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationConnectionTester "github.com/rudderlabs/rudder-server/services/destination-connection-tester"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/tidwall/gjson"
)

var (
	webPort                             int
	dbHandle                            *sql.DB
	notifier                            pgnotifier.PgNotifierT
	WarehouseDestinations               []string
	jobQueryBatchSize                   int
	noOfWorkers                         int
	noOfSlaveWorkerRoutines             int
	slaveWorkerRoutineBusy              []bool //Busy-true
	uploadFreqInS                       int64
	stagingFilesSchemaPaginationSize    int
	mainLoopSleep                       time.Duration
	workerRetrySleep                    time.Duration
	stagingFilesBatchSize               int
	configSubscriberLock                sync.RWMutex
	crashRecoverWarehouses              []string
	inProgressMap                       map[string]bool
	inRecoveryMap                       map[string]bool
	inProgressMapLock                   sync.RWMutex
	lastExecMap                         map[string]int64
	lastExecMapLock                     sync.RWMutex
	warehouseMode                       string
	warehouseSyncPreFetchCount          int
	warehouseSyncFreqIgnore             bool
	activeWorkerCount                   int
	activeWorkerCountLock               sync.RWMutex
	minRetryAttempts                    int
	retryTimeWindow                     time.Duration
	maxStagingFileReadBufferCapacityInK int
)

var (
	host, user, password, dbname, sslmode string
	port                                  int
)

// warehouses worker modes
const (
	MasterMode      = "master"
	SlaveMode       = "slave"
	MasterSlaveMode = "master_and_slave"
	EmbeddedMode    = "embedded"
)

const StagingFileProcessPGChannel = "process_staging_file"

type HandleT struct {
	destType             string
	warehouses           []warehouseutils.WarehouseT
	dbHandle             *sql.DB
	notifier             pgnotifier.PgNotifierT
	uploadToWarehouseQ   chan []ProcessStagingFilesJobT
	createLoadFilesQ     chan LoadFileJobT
	isEnabled            bool
	configSubscriberLock sync.RWMutex
	workerChannelMap     map[string]chan []*UploadJobT
	workerChannelMapLock sync.RWMutex
}

type ErrorResponseT struct {
	Error string
}

func init() {
	loadConfig()
}

func loadConfig() {
	//Port where WH is running
	webPort = config.GetInt("Warehouse.webPort", 8082)
	WarehouseDestinations = []string{"RS", "BQ", "SNOWFLAKE", "POSTGRES", "CLICKHOUSE"}
	jobQueryBatchSize = config.GetInt("Router.jobQueryBatchSize", 10000)
	noOfWorkers = config.GetInt("Warehouse.noOfWorkers", 8)
	noOfSlaveWorkerRoutines = config.GetInt("Warehouse.noOfSlaveWorkerRoutines", 4)
	stagingFilesBatchSize = config.GetInt("Warehouse.stagingFilesBatchSize", 240)
	uploadFreqInS = config.GetInt64("Warehouse.uploadFreqInS", 1800)
	mainLoopSleep = config.GetDuration("Warehouse.mainLoopSleepInS", 60) * time.Second
	workerRetrySleep = config.GetDuration("Warehouse.workerRetrySleepInS", 5) * time.Second
	crashRecoverWarehouses = []string{"RS"}
	inProgressMap = map[string]bool{}
	inRecoveryMap = map[string]bool{}
	lastExecMap = map[string]int64{}
	warehouseMode = config.GetString("Warehouse.mode", "embedded")
	host = config.GetEnv("WAREHOUSE_JOBS_DB_HOST", "localhost")
	user = config.GetEnv("WAREHOUSE_JOBS_DB_USER", "ubuntu")
	dbname = config.GetEnv("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")
	port, _ = strconv.Atoi(config.GetEnv("WAREHOUSE_JOBS_DB_PORT", "5432"))
	password = config.GetEnv("WAREHOUSE_JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
	sslmode = config.GetEnv("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	warehouseSyncPreFetchCount = config.GetInt("Warehouse.warehouseSyncPreFetchCount", 10)
	stagingFilesSchemaPaginationSize = config.GetInt("Warehouse.stagingFilesSchemaPaginationSize", 100)
	warehouseSyncFreqIgnore = config.GetBool("Warehouse.warehouseSyncFreqIgnore", false)
	minRetryAttempts = config.GetInt("Warehouse.minRetryAttempts", 3)
	retryTimeWindow = config.GetDuration("Warehouse.retryTimeWindowInMins", time.Duration(180)) * time.Minute
	maxStagingFileReadBufferCapacityInK = config.GetInt("Warehouse.maxStagingFileReadBufferCapacityInK", 1024)
}

// get name of the worker (`destID_namespace`) to be stored in map wh.workerChannelMap
func workerIdentifier(warehouse warehouseutils.WarehouseT) string {
	return fmt.Sprintf(`%s_%s`, warehouse.Destination.ID, warehouse.Namespace)
}

func (wh *HandleT) handleUploadJobs(jobs []*UploadJobT) {
	// infinite loop to check for active workers count and retry if not
	// break after handling
	for {
		// check number of workers actively enagaged
		// if limit hit, sleep and check again
		// activeWorkerCount is across all wh.destType's
		activeWorkerCountLock.Lock()
		activeWorkers := activeWorkerCount
		if activeWorkers >= noOfWorkers {
			activeWorkerCountLock.Unlock()
			logger.Debugf("[WH]: Setting to sleep and waiting till activeWorkers are less than %d", noOfWorkers)
			// TODO: add randomness to this ?
			time.Sleep(workerRetrySleep)
			continue
		}
		activeWorkerCount++
		activeWorkerCountLock.Unlock()
		break
	}

	// TODO: Is this metric required?
	whOneFullPassTimer := warehouseutils.DestStat(stats.TimerType, "total_end_to_end_step_time", jobs[0].warehouse.Destination.ID)
	whOneFullPassTimer.Start()
	for _, job := range jobs {
		err := job.run()
		wh.recordDeliveryStatus(job.warehouse.Destination.ID, job.upload.ID)
		if err != nil {
			warehouseutils.DestStat(stats.CountType, "failed_uploads", job.warehouse.Destination.ID).Count(1)
			// do not process other jobs so that uploads are done in order
			break
		}
		onSuccessfulUpload(job.warehouse)
		// TODO: Is this metric required?
		warehouseutils.DestStat(stats.CountType, "load_staging_files_into_warehouse", job.warehouse.Destination.ID).Count(len(job.stagingFiles))
	}
	whOneFullPassTimer.End()

	// decrement number of workers actively engaged
	activeWorkerCountLock.Lock()
	activeWorkerCount--
	activeWorkerCountLock.Unlock()
}

func (wh *HandleT) initWorker(identifier string) chan []*UploadJobT {
	workerChan := make(chan []*UploadJobT, 100)
	rruntime.Go(func() {
		for {
			uploads := <-workerChan
			wh.handleUploadJobs(uploads)
			setDestInProgress(uploads[0].warehouse, false)
		}
	})
	return workerChan
}

func (wh *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		wh.warehouses = []warehouseutils.WarehouseT{}
		allSources := config.Data.(backendconfig.SourcesT)

		for _, source := range allSources.Sources {
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.DestinationDefinition.Name == wh.destType {
						namespace := wh.getNamespace(destination.Config, source, destination, wh.destType)
						warehouse := warehouseutils.WarehouseT{Source: source, Destination: destination, Namespace: namespace, Type: wh.destType, Identifier: fmt.Sprintf("%s:%s:%s", wh.destType, source.ID, destination.ID)}
						wh.warehouses = append(wh.warehouses, warehouse)

						workerName := workerIdentifier(warehouse)
						wh.workerChannelMapLock.Lock()
						// spawn one worker for each unique destID_namespace
						// check this commit to https://github.com/rudderlabs/rudder-server/pull/476/commits/4a0a10e5faa2c337c457f14c3ad1c32e2abfb006
						// to avoid creating goroutine for disabled sources/destiantions
						if _, ok := wh.workerChannelMap[workerName]; !ok {
							workerChan := wh.initWorker(workerName)
							wh.workerChannelMap[workerName] = workerChan
						}
						wh.workerChannelMapLock.Unlock()

						// send last 10 warehouse upload's status to control plane
						if destination.Config != nil && destination.Enabled && destination.Config["eventDelivery"] == true {
							sourceID := source.ID
							destinationID := destination.ID
							rruntime.Go(func() {
								wh.syncLiveWarehouseStatus(sourceID, destinationID)
							})
						}
						// test and send connection status to control plane
						if val, ok := destination.Config["testConnection"].(bool); ok && val {
							destination := destination
							rruntime.Go(func() {
								testResponse := destinationConnectionTester.TestWarehouseDestinationConnection(destination)
								destinationConnectionTester.UploadDestinationConnectionTesterResponse(testResponse, destination.ID)
							})
						}

						if warehouseutils.IDResolutionEnabled() && misc.ContainsString(warehouseutils.IdentityEnabledWarehouses, warehouse.Type) {
							wh.setupIdentityTables(warehouse)
							if shouldPopulateHistoricIdentities && warehouse.Destination.Enabled {
								// non blocking populate historic identities
								wh.populateHistoricIdentities(warehouse)
							}
						}
					}
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

// getNamespace sets namespace name in the following order
// 	1. user set name from destinationConfig
// 	2. from existing record in wh_schemas with same source + dest combo
// 	3. convert source name
func (wh *HandleT) getNamespace(config interface{}, source backendconfig.SourceT, destination backendconfig.DestinationT, destType string) string {
	configMap := config.(map[string]interface{})
	var namespace string
	if destType == "CLICKHOUSE" {
		//TODO: Handle if configMap["database"] is nil
		return configMap["database"].(string)
	}
	if configMap["namespace"] != nil {
		namespace = configMap["namespace"].(string)
		if len(strings.TrimSpace(namespace)) > 0 {
			return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, namespace))
		}
	}
	var exists bool
	if namespace, exists = warehouseutils.GetNamespace(source, destination, wh.dbHandle); !exists {
		namespace = warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, source.Name))
	}
	return namespace
}

func (wh *HandleT) getStagingFiles(warehouse warehouseutils.WarehouseT, startID int64, endID int64) ([]*StagingFileT, error) {
	sqlStatement := fmt.Sprintf(`SELECT id, location
                                FROM %[1]s
								WHERE %[1]s.id >= %[2]v AND %[1]s.id <= %[3]v AND %[1]s.source_id='%[4]s' AND %[1]s.destination_id='%[5]s'
								ORDER BY id ASC`,
		warehouseutils.WarehouseStagingFilesTable, startID, endID, warehouse.Source.ID, warehouse.Destination.ID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location)
		if err != nil {
			panic(err)
		}
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}

func (wh *HandleT) getPendingStagingFiles(warehouse warehouseutils.WarehouseT) ([]*StagingFileT, error) {
	var lastStagingFileID int64
	sqlStatement := fmt.Sprintf(`SELECT end_staging_file_id FROM %[1]s WHERE %[1]s.destination_type='%[2]s' AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id='%[4]s' AND (%[1]s.status= '%[5]s' OR %[1]s.status = '%[6]s') ORDER BY %[1]s.id DESC`, warehouseutils.WarehouseUploadsTable, warehouse.Type, warehouse.Source.ID, warehouse.Destination.ID, ExportedDataState, AbortedState)

	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&lastStagingFileID)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`SELECT id, location, first_event_at, last_event_at
                                FROM %[1]s
								WHERE %[1]s.id > %[2]v AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id='%[4]s'
								ORDER BY id ASC`,
		warehouseutils.WarehouseStagingFilesTable, lastStagingFileID, warehouse.Source.ID, warehouse.Destination.ID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	var firstEventAt, lastEventAt sql.NullTime
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &firstEventAt, &lastEventAt)
		if err != nil {
			panic(err)
		}
		jsonUpload.FirstEventAt = firstEventAt.Time
		jsonUpload.LastEventAt = lastEventAt.Time
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}

func (wh *HandleT) initUpload(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT) UploadT {
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (source_id, namespace, destination_id, destination_type, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, status, schema, error, first_event_at, last_event_at, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10, $11, $12, $13, $14, $15) RETURNING id`, warehouseutils.WarehouseUploadsTable)
	logger.Infof("[WH]: %s: Creating record in %s table: %v", wh.destType, warehouseutils.WarehouseUploadsTable, sqlStatement)
	stmt, err := wh.dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	startJSONID := jsonUploadsList[0].ID
	endJSONID := jsonUploadsList[len(jsonUploadsList)-1].ID
	namespace := warehouse.Namespace

	var firstEventAt, lastEventAt time.Time
	if ok := jsonUploadsList[0].FirstEventAt.IsZero(); !ok {
		firstEventAt = jsonUploadsList[0].FirstEventAt
	}
	if ok := jsonUploadsList[len(jsonUploadsList)-1].LastEventAt.IsZero(); !ok {
		lastEventAt = jsonUploadsList[len(jsonUploadsList)-1].LastEventAt
	}

	now := timeutil.Now()
	row := stmt.QueryRow(warehouse.Source.ID, namespace, warehouse.Destination.ID, wh.destType, startJSONID, endJSONID, 0, 0, WaitingState, "{}", "{}", firstEventAt, lastEventAt, now, now)

	var uploadID int64
	err = row.Scan(&uploadID)
	if err != nil {
		panic(err)
	}

	upload := UploadT{
		ID:                 uploadID,
		Namespace:          warehouse.Namespace,
		SourceID:           warehouse.Source.ID,
		DestinationID:      warehouse.Destination.ID,
		DestinationType:    wh.destType,
		StartStagingFileID: startJSONID,
		EndStagingFileID:   endJSONID,
		Status:             WaitingState,
	}

	return upload
}

func (wh *HandleT) getPendingUploads(warehouse warehouseutils.WarehouseT) ([]UploadT, error) {

	sqlStatement := fmt.Sprintf(`SELECT id, status, schema, namespace, source_id, destination_id, destination_type, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, error, timings->0 as firstTiming, timings->-1 as lastTiming FROM %[1]s WHERE (%[1]s.destination_type='%[2]s' AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id = '%[4]s' AND %[1]s.status != '%[5]s' AND %[1]s.status != '%[6]s') ORDER BY id asc`, warehouseutils.WarehouseUploadsTable, wh.destType, warehouse.Source.ID, warehouse.Destination.ID, ExportedDataState, AbortedState)

	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		return []UploadT{}, err
	}

	if err == sql.ErrNoRows {
		return []UploadT{}, nil
	}
	defer rows.Close()

	var uploads []UploadT
	for rows.Next() {
		var upload UploadT
		var schema json.RawMessage
		var firstTiming sql.NullString
		var lastTiming sql.NullString
		err := rows.Scan(&upload.ID, &upload.Status, &schema, &upload.Namespace, &upload.SourceID, &upload.DestinationID, &upload.DestinationType, &upload.StartStagingFileID, &upload.EndStagingFileID, &upload.StartLoadFileID, &upload.EndLoadFileID, &upload.Error, &firstTiming, &lastTiming)
		if err != nil {
			panic(err)
		}
		upload.Schema = warehouseutils.JSONSchemaToMap(schema)

		_, upload.FirstAttemptAt = warehouseutils.TimingFromJSONString(firstTiming)
		var lastStatus string
		lastStatus, upload.LastAttemptAt = warehouseutils.TimingFromJSONString(lastTiming)
		upload.Attempts = gjson.Get(string(upload.Error), fmt.Sprintf(`%s.attempt`, lastStatus)).Int()

		uploads = append(uploads, upload)
	}

	return uploads, nil
}

func connectionString(warehouse warehouseutils.WarehouseT) string {
	return fmt.Sprintf(`source:%s:destination:%s`, warehouse.Source.ID, warehouse.Destination.ID)
}

func setDestInProgress(warehouse warehouseutils.WarehouseT, starting bool) {
	inProgressMapLock.Lock()
	defer inProgressMapLock.Unlock()
	if starting {
		inProgressMap[connectionString(warehouse)] = true
	} else {
		delete(inProgressMap, connectionString(warehouse))
	}
}

func isDestInProgress(warehouse warehouseutils.WarehouseT) bool {
	inProgressMapLock.RLock()
	defer inProgressMapLock.RUnlock()
	if inProgressMap[connectionString(warehouse)] {
		return true
	}
	return false
}

func uploadFrequencyExceeded(warehouse warehouseutils.WarehouseT, syncFrequency string) bool {
	freqInS := uploadFreqInS
	if syncFrequency != "" {
		freqInMin, _ := strconv.ParseInt(syncFrequency, 10, 64)
		freqInS = freqInMin * 60
	}
	lastExecMapLock.Lock()
	defer lastExecMapLock.Unlock()
	if lastExecTime, ok := lastExecMap[connectionString(warehouse)]; ok && timeutil.Now().Unix()-lastExecTime < freqInS {
		return true
	}
	return false
}

func setLastExec(warehouse warehouseutils.WarehouseT) {
	lastExecMapLock.Lock()
	defer lastExecMapLock.Unlock()
	lastExecMap[connectionString(warehouse)] = timeutil.Now().Unix()
}

func (wh *HandleT) getUploadJobsForPendingUploads(warehouse warehouseutils.WarehouseT, whManager manager.ManagerI, pendingUploads []UploadT) ([]*UploadJobT, error) {
	uploadJobs := []*UploadJobT{}
	for _, pendingUpload := range pendingUploads {
		if !wh.canStartPendingUpload(pendingUpload, warehouse) {
			logger.Debugf("[WH]: Skipping pending upload for %s since current time less than next retry time", warehouse.Identifier)
			break
		}
		stagingFilesList, err := wh.getStagingFiles(warehouse, pendingUpload.StartStagingFileID, pendingUpload.EndStagingFileID)
		if err != nil {
			return uploadJobs, err
		}

		uploadJob := UploadJobT{
			upload:       &pendingUpload,
			stagingFiles: stagingFilesList,
			warehouse:    warehouse,
			whManager:    whManager,
			dbHandle:     wh.dbHandle,
			pgNotifier:   &wh.notifier,
		}

		logger.Debugf("[WH]: Adding job %+v", uploadJob)
		uploadJobs = append(uploadJobs, &uploadJob)
	}

	return uploadJobs, nil
}

func (wh *HandleT) getUploadJobsForNewStagingFiles(warehouse warehouseutils.WarehouseT, whManager manager.ManagerI, stagingFilesList []*StagingFileT) ([]*UploadJobT, error) {
	count := 0
	var uploadJobs []*UploadJobT
	// Process staging files in batches of stagingFilesBatchSize
	// Eg. If there are 1000 pending staging files and stagingFilesBatchSize is 100,
	// Then we create 10 new entries in wh_uploads table each with 100 staging files
	for {
		lastIndex := count + stagingFilesBatchSize
		if lastIndex >= len(stagingFilesList) {
			lastIndex = len(stagingFilesList)
		}

		upload := wh.initUpload(warehouse, stagingFilesList[count:lastIndex])

		job := UploadJobT{
			upload:       &upload,
			stagingFiles: stagingFilesList[count:lastIndex],
			warehouse:    warehouse,
			whManager:    whManager,
			dbHandle:     wh.dbHandle,
			pgNotifier:   &wh.notifier,
		}

		uploadJobs = append(uploadJobs, &job)
		count += stagingFilesBatchSize
		if count >= len(stagingFilesList) {
			break
		}
	}

	return uploadJobs, nil
}

func (wh *HandleT) processWarehouseJobs(warehouse warehouseutils.WarehouseT) error {
	if isDestInProgress(warehouse) {
		logger.Debugf("[WH]: Skipping upload loop since %s upload in progress", warehouse.Identifier)
		return nil
	}

	enqueuedJobs := false
	setDestInProgress(warehouse, true)
	defer func() {
		if !enqueuedJobs {
			setDestInProgress(warehouse, false)
		}
	}()

	whManager, err := manager.New(wh.destType)
	if err != nil {
		return err
	}

	// Step 1: Crash recovery after restart
	// Remove pending temp tables in Redshift etc.
	_, ok := inRecoveryMap[warehouse.Destination.ID]
	if ok {
		logger.Infof("[WH]: Crash recovering for %s:%s", wh.destType, warehouse.Destination.ID)
		err = whManager.CrashRecover(warehouse)
		if err != nil {
			return err
		}
		delete(inRecoveryMap, warehouse.Destination.ID)
	}

	var uploadJobs []*UploadJobT

	// Step 2: Handle any Pending uploads
	// An upload job is pending if it is neither exported nor aborted

	pendingUploads, err := wh.getPendingUploads(warehouse)
	if err != nil {
		logger.Errorf("[WH]: Failed to get pending uploads: %s with error %w", warehouse.Identifier, err)
		return err
	}

	if len(pendingUploads) > 0 {
		logger.Infof("[WH]: Found pending uploads: %v for %s", len(pendingUploads), warehouse.Identifier)
		uploadJobs, err = wh.getUploadJobsForPendingUploads(warehouse, whManager, pendingUploads)
		if err != nil {
			logger.Errorf("[WH]: Failed to create upload jobs for %s from pending uploads with error: %w", warehouse.Identifier, err)
			return err
		}
		wh.enqueueUploadJobs(uploadJobs, warehouse)
		enqueuedJobs = true
		return nil
	}

	// Step 3: Handle pending staging files. Create new uploads for them
	// We will perform only one of Step 2 or Step 3, in every execution

	if !wh.canStartUpload(warehouse) {
		logger.Debugf("[WH]: Skipping upload loop since %s upload freq not exceeded", warehouse.Identifier)
		return nil
	}

	stagingFilesList, err := wh.getPendingStagingFiles(warehouse)
	if err != nil {
		logger.Errorf("[WH]: Failed to get pending staging files: %s with error %w", warehouse.Identifier, err)
		return err
	}
	if len(stagingFilesList) == 0 {
		logger.Debugf("[WH]: Found no pending staging files for %s", warehouse.Identifier)
		return nil
	}

	uploadJobs, err = wh.getUploadJobsForNewStagingFiles(warehouse, whManager, stagingFilesList)
	if err != nil {
		logger.Errorf("[WH]: Failed to create upload jobs for %s for new staging files with error: %w", warehouse.Identifier, err)
		return err
	}

	setLastExec(warehouse)
	wh.enqueueUploadJobs(uploadJobs, warehouse)
	enqueuedJobs = true
	return nil
}

func (wh *HandleT) mainLoop() {
	for {
		time.Sleep(mainLoopSleep)

		wh.configSubscriberLock.RLock()
		if !wh.isEnabled {
			wh.configSubscriberLock.RUnlock()
			continue
		}
		wh.configSubscriberLock.RUnlock()

		configSubscriberLock.RLock()
		warehouses := wh.warehouses
		configSubscriberLock.RUnlock()
		for _, warehouse := range warehouses {
			err := wh.processWarehouseJobs(warehouse)
			if err != nil {
				logger.Errorf("[WH] Failed to process warehouse Jobs: %w", err)
			}
		}
	}
}

func (wh *HandleT) enqueueUploadJobs(uploads []*UploadJobT, warehouse warehouseutils.WarehouseT) {
	if len(uploads) == 0 {
		logger.Errorf("[WH]: Zero upload jobs, not enqueuing")
		return
	}
	workerName := workerIdentifier(warehouse)
	wh.workerChannelMapLock.Lock()
	wh.workerChannelMap[workerName] <- uploads
	wh.workerChannelMapLock.Unlock()
}

func getBucketFolder(batchID string, tableName string) string {
	return fmt.Sprintf(`%v-%v`, batchID, tableName)
}

//Enable enables a router :)
func (wh *HandleT) Enable() {
	wh.isEnabled = true
}

//Disable disables a router:)
func (wh *HandleT) Disable() {
	wh.isEnabled = false
}

func (wh *HandleT) setInterruptedDestinations() (err error) {
	if !misc.Contains(crashRecoverWarehouses, wh.destType) {
		return
	}
	sqlStatement := fmt.Sprintf(`SELECT destination_id FROM %s WHERE destination_type='%s' AND (status='%s' OR status='%s')`, warehouseutils.WarehouseUploadsTable, wh.destType, ExportingDataState, ExportingDataFailedState)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var destID string
		err := rows.Scan(&destID)
		if err != nil {
			panic(err)
		}
		inRecoveryMap[destID] = true
	}
	return err
}

func (wh *HandleT) Setup(whType string) {
	logger.Infof("[WH]: Warehouse Router started: %s", whType)
	wh.dbHandle = dbHandle
	wh.notifier = notifier
	wh.destType = whType
	wh.setInterruptedDestinations()
	wh.Enable()
	wh.uploadToWarehouseQ = make(chan []ProcessStagingFilesJobT)
	wh.createLoadFilesQ = make(chan LoadFileJobT)
	wh.workerChannelMap = make(map[string]chan []*UploadJobT)
	rruntime.Go(func() {
		wh.backendConfigSubscriber()
	})
	rruntime.Go(func() {
		wh.mainLoop()
	})
}

var loadFileFormatMap = map[string]string{
	"BQ":         "json",
	"RS":         "csv",
	"SNOWFLAKE":  "csv",
	"POSTGRES":   "csv",
	"CLICKHOUSE": "csv",
}

// Gets the config from config backend and extracts enabled writekeys
func monitorDestRouters() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	dstToWhRouter := make(map[string]*HandleT)

	for {
		config := <-ch
		logger.Debug("Got config from config-backend", config)
		sources := config.Data.(backendconfig.SourcesT)
		enabledDestinations := make(map[string]bool)
		for _, source := range sources.Sources {
			for _, destination := range source.Destinations {
				enabledDestinations[destination.DestinationDefinition.Name] = true
				if misc.Contains(WarehouseDestinations, destination.DestinationDefinition.Name) {
					wh, ok := dstToWhRouter[destination.DestinationDefinition.Name]
					if !ok {
						logger.Info("Starting a new Warehouse Destination Router: ", destination.DestinationDefinition.Name)
						var wh HandleT
						wh.configSubscriberLock.Lock()
						wh.Setup(destination.DestinationDefinition.Name)
						wh.configSubscriberLock.Unlock()
						dstToWhRouter[destination.DestinationDefinition.Name] = &wh
					} else {
						logger.Debug("Enabling existing Destination: ", destination.DestinationDefinition.Name)
						wh.configSubscriberLock.Lock()
						wh.Enable()
						wh.configSubscriberLock.Unlock()
					}
				}
			}
		}

		keys := misc.StringKeys(dstToWhRouter)
		for _, key := range keys {
			if _, ok := enabledDestinations[key]; !ok {
				if wh, ok := dstToWhRouter[key]; ok {
					logger.Info("Disabling a existing warehouse destination: ", key)
					wh.configSubscriberLock.Lock()
					wh.Disable()
					wh.configSubscriberLock.Unlock()
				}
			}
		}
	}
}

func setupTables(dbHandle *sql.DB) {
	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "wh_schema_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", false),
	}

	err := m.Migrate("warehouse")
	if err != nil {
		panic(fmt.Errorf("Could not run warehouse database migrations: %w", err))
	}
}

func CheckPGHealth() bool {
	rows, err := dbHandle.Query(fmt.Sprintf(`SELECT 'Rudder Warehouse DB Health Check'::text as message`))
	if err != nil {
		logger.Error(err)
		return false
	}
	defer rows.Close()
	return true
}

func processHandler(w http.ResponseWriter, r *http.Request) {
	logger.LogRequest(r)

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var stagingFile warehouseutils.StagingFileT
	json.Unmarshal(body, &stagingFile)

	var firstEventAt, lastEventAt interface{}
	firstEventAt = stagingFile.FirstEventAt
	lastEventAt = stagingFile.LastEventAt
	if stagingFile.FirstEventAt == "" || stagingFile.LastEventAt == "" {
		firstEventAt = nil
		lastEventAt = nil
	}

	logger.Debugf("[WH]: Creating staging file entry in %s table with schema: %+v", warehouseutils.WarehouseStagingFilesTable, stagingFile.Schema)
	schemaPayload, err := json.Marshal(stagingFile.Schema)
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (location, schema, source_id, destination_id, status, total_events, first_event_at, last_event_at, created_at, updated_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`, warehouseutils.WarehouseStagingFilesTable)
	stmt, err := dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(stagingFile.Location, schemaPayload, stagingFile.BatchDestination.Source.ID, stagingFile.BatchDestination.Destination.ID, warehouseutils.StagingFileWaitingState, stagingFile.TotalEvents, firstEventAt, lastEventAt, timeutil.Now())
	if err != nil {
		panic(err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	var dbService string = "UP"
	if !CheckPGHealth() {
		dbService = "DOWN"
	}
	healthVal := fmt.Sprintf(`{"server":"UP", "db":"%s","acceptingEvents":"TRUE","warehouseMode":"%s","goroutines":"%d"}`, dbService, strings.ToUpper(warehouseMode), runtime.NumGoroutine())
	w.Write([]byte(healthVal))
}

func getConnectionString() string {
	if warehouseMode == config.EmbeddedMode {
		return jobsdb.GetConnectionString()
	}
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)
}

func startWebHandler() {
	// do not register same endpoint when running embedded in rudder backend
	if isStandAlone() {
		http.HandleFunc("/health", healthHandler)
	}
	if isMaster() {
		backendconfig.WaitForConfig()
		http.HandleFunc("/v1/process", processHandler)
		logger.Infof("[WH]: Starting warehouse master service in %d", webPort)
	} else {
		logger.Infof("[WH]: Starting warehouse slave service in %d", webPort)
	}
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(webPort), bugsnag.Handler(nil)))
}

func isStandAlone() bool {
	return warehouseMode != EmbeddedMode
}

func isMaster() bool {
	return warehouseMode == config.MasterMode || warehouseMode == config.MasterSlaveMode || warehouseMode == config.EmbeddedMode
}

func isSlave() bool {
	return warehouseMode == config.SlaveMode || warehouseMode == config.MasterSlaveMode || warehouseMode == config.EmbeddedMode
}

func Start() {
	time.Sleep(1 * time.Second)
	// do not start warehouse service if rudder core is not in normal mode and warehouse is running in same process as rudder core
	if !isStandAlone() && !db.IsNormalMode() {
		logger.Infof("Skipping start of warehouse service...")
		return
	}

	logger.Infof("[WH]: Starting Warehouse service...")
	var err error
	psqlInfo := getConnectionString()

	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	if !validators.IsPostgresCompatible(dbHandle) {
		err := errors.New("Rudder Warehouse Service needs postgres version >= 10. Exiting")
		logger.Error(err)
		panic(err)
	}

	setupTables(dbHandle)

	notifier, err = pgnotifier.New(psqlInfo)
	if err != nil {
		panic(err)
	}

	if isSlave() {
		logger.Infof("[WH]: Starting warehouse slave...")
		setupSlave()
	}

	if isMaster() {
		logger.Infof("[WH]: Starting warehouse master...")
		err = notifier.AddTopic(StagingFileProcessPGChannel)
		if err != nil {
			panic(err)
		}
		rruntime.Go(func() {
			monitorDestRouters()
		})
	}

	startWebHandler()
}
