package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type transformerResponseT struct {
	output     []byte
	err        string
	statusCode int
}

type transformerBatchResponseT struct {
	batchError error
	responses  []transformerResponseT
	statusCode int
}

func (bt *batchWebhookTransformerT) transform(events [][]byte, sourceType string) transformerBatchResponseT {
	bt.stats.sentStat.Count(len(events))
	bt.stats.transformTimerStat.Start()

	payload := misc.MakeJSONArray(events)
	url := fmt.Sprintf(`%s/%s`, sourceTransformerURL, strings.ToLower(sourceType))
	resp, err := bt.webhook.netClient.Post(url, "application/json; charset=utf-8", bytes.NewBuffer(payload))

	bt.stats.transformTimerStat.End()
	if err != nil {
		logger.Error(err)
		bt.stats.failedStat.Count(len(events))
		return transformerBatchResponseT{batchError: errors.New("Internal server error in source transformer")}
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	if err != nil {
		bt.stats.failedStat.Count(len(events))
		return transformerBatchResponseT{batchError: err}
	}

	var responses []interface{}
	err = json.Unmarshal(respBody, &responses)

	if err != nil {
		return transformerBatchResponseT{
			batchError: errors.New(getStatus(SourceTransformerInvalidResponseFormat)),
			statusCode: getStatusCode(SourceTransformerInvalidResponseFormat),
		}
	}

	batchResponse := transformerBatchResponseT{responses: make([]transformerResponseT, len(events))}

	if len(responses) != len(events) {
		panic("Source rudder-transformer response size does not equal sent events size")
	}

	for idx, response := range responses {
		respElemMap, castOk := response.(map[string]interface{})
		if castOk {
			outputInterface, ok := respElemMap["output"]
			if !ok {
				batchResponse.responses[idx] = transformerResponseT{
					err:        getStatus(SourceTransformerFailedToReadOutput),
					statusCode: getStatusCode(SourceTransformerFailedToReadOutput),
				}
				bt.stats.failedStat.Count(1)
				continue
			}

			output, ok := outputInterface.(map[string]interface{})
			if !ok {
				batchResponse.responses[idx] = transformerResponseT{
					err:        getStatus(SourceTransformerInvalidOutputFormatInResponse),
					statusCode: getStatusCode(SourceTransformerInvalidOutputFormatInResponse),
				}
				bt.stats.failedStat.Count(1)
				continue
			}

			if statusCode, found := output["statusCode"]; found && fmt.Sprintf("%v", statusCode) != "200" {
				var errorMessage interface{}
				code, _ := statusCode.(int)
				if errorMessage, ok = output["error"]; !ok {
					errorMessage = getStatus(SourceTrasnformerResponseErrorReadFailed)
				}
				batchResponse.responses[idx] = transformerResponseT{
					err:        fmt.Sprintf("%v", errorMessage),
					statusCode: code,
				}
				bt.stats.failedStat.Count(1)
				continue
			}
			bt.stats.receivedStat.Count(1)
			marshalledOutput, _ := json.Marshal(output)
			batchResponse.responses[idx] = transformerResponseT{output: marshalledOutput}
		} else {
			batchResponse.responses[idx] = transformerResponseT{
				err:        getStatus(SourceTransformerInvalidResponseFormat),
				statusCode: getStatusCode(SourceTransformerInvalidResponseFormat),
			}
			bt.stats.failedStat.Count(1)
		}
	}
	return batchResponse
}