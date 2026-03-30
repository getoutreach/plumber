package format

import "testing"

func TestToCamelName(t *testing.T) {
	pairs := map[string]string{
		"id":       "id",
		"neco_id":  "necoId",
		"uuid":     "uuid",
		"someName": "someName",
	}
	for raw, formated := range pairs {
		converted := CamelCase(raw)
		if converted != formated {
			t.Errorf("%s is converted to %s and not to %s", raw, converted, formated)
		}
	}
}

func TestToStructName(t *testing.T) {
	pairs := map[string]string{
		"id":          "ID",
		"neco_id":     "NecoID",
		"necoID":      "NecoID",
		"uuid":        "UUID",
		"someName":    "SomeName",
		"description": "Description",
		"uint32":      "Uint32",
	}
	for raw, formated := range pairs {
		converted := ToStructName(raw)
		if converted != formated {
			t.Errorf("%s is converted to %s and not to %s", raw, converted, formated)
		}
	}
}

func TestProtoPascalCaseName(t *testing.T) {
	pairs := map[string]string{
		"created_at":     "CreatedAt",
		"pricebook_2_id": "Pricebook_2Id",
		"pricebook_id":   "PricebookId",
	}
	for raw, formated := range pairs {
		converted := ProtoPascalCase(raw)
		if converted != formated {
			t.Errorf("%s is converted to %s and not to %s", raw, converted, formated)
		}
	}
}
