package event_schema

import (
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type eventSchemasFeatureImpl struct {
}

// Setup initializes EventSchemas feature
func (m *eventSchemasFeatureImpl) Setup() types.EventSchemasI {
	logger.Info("[[ EventSchemas ]] Setting up EventSchemas Feature")
	eventSchemaManager := &EventSchemaManagerT{}
	eventSchemaManager.Setup()

	return eventSchemaManager
}
