package event_schema

import (
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

// Setup initializes EventSchemas feature
func Setup() types.EventSchemasI {
	logger.Info("[[ EventSchemas ]] Setting up EventSchemas FeatureValue")
	eventSchemaManager := &EventSchemaManagerT{}
	eventSchemaManager.Setup()

	return eventSchemaManager
}
