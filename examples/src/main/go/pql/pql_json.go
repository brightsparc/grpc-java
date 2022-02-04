package pql

import (
	"encoding/json"
	"fmt"
)

func (i *GivenItem) MarshalJSON() ([]byte, error) {
	// Custom marshaling to return the ray types for a GivenItem
	switch i.Type {
	case GivenItem_IDENTIFIER:
		return json.Marshal(i.IdentifierValue[0])
	case GivenItem_STRING:
		return json.Marshal(i.StringValue[0])
	case GivenItem_NUMERIC:
		return json.Marshal(i.NumericValue[0])
	case GivenItem_BINARY:
		return json.Marshal(i.BoolValue[0])
	case GivenItem_ARRAY:
		// Check which type we have for this array
		if len(i.StringValue) > 0 {
			return json.Marshal(i.StringValue)
		} else if len(i.NumericValue) > 0 {
			return json.Marshal(i.NumericValue)
		} else if len(i.BoolValue) > 0 {
			return json.Marshal(i.BoolValue)
		}
		return nil, fmt.Errorf("unexpected entries for %s", i.Type.String())
	// Write out a format that matches hyper parameters
	// see: https://ludwig-ai.github.io/ludwig-docs/0.4/user_guide/hyperparameter_optimization/
	case GivenItem_SAMPLE_ARRAY:
		var sampleType string
		if *i.SampleType == GivenItem_SAMPLE_CHOICE {
			sampleType = "choice"
		} else {
			sampleType = "grid"
		}
		if len(i.StringValue) > 0 {
			return json.Marshal(struct {
				Space  string   `json:"space,omitempty"`
				Values []string `json:"values,omitempty"`
			}{
				sampleType,
				i.StringValue,
			})
		} else if len(i.NumericValue) > 0 {
			return json.Marshal(struct {
				Space  string    `json:"space,omitempty"`
				Values []float64 `json:"values,omitempty"`
			}{
				sampleType,
				i.NumericValue,
			})
		} else if len(i.BoolValue) > 0 {
			return json.Marshal(struct {
				Space  string `json:"space,omitempty"`
				Values []bool `json:"values,omitempty"`
			}{
				sampleType,
				i.BoolValue,
			})
		}
		return nil, fmt.Errorf("unexpected entries for %s", i.Type.String())
	case GivenItem_RANGE_INT:
		value := struct {
			Type string   `json:"type,omitempty"`
			Min  *float64 `json:"min,omitempty"`
			Max  *float64 `json:"max,omitempty"`
			Step *float64 `json:"step,omitempty"`
		}{
			"int",
			i.MinValue, i.MaxValue, i.StepValue,
		}
		return json.Marshal(value)
	case GivenItem_RANGE_REAL:
		var scaleType string
		if i.ScaleType != nil && *i.ScaleType != GivenItem_AUTO {
			scaleType = i.ScaleType.String()
		}
		value := struct {
			Type  string   `json:"type,omitempty"`
			Low   *float64 `json:"low,omitempty"`
			High  *float64 `json:"high,omitempty"`
			Steps *float64 `json:"steps,omitempty"`
			Scale string   `json:"scale,omitempty"`
		}{
			"real",
			i.MinValue, i.MaxValue, i.StepValue, // empty step wont print
			scaleType, // empty string won't print out
		}
		return json.Marshal(value)
	default:
		return nil, fmt.Errorf("type %s not supported", i.Type.String())
	}
}
