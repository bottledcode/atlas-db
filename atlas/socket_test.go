package atlas

import "testing"

func TestCommandFromString(t *testing.T) {
	command := "SELECT * FROM table"
	cs := commandFromString(command)

	if cs.raw != command {
		t.Errorf("expected raw command to be %s, got %s", command, cs.raw)
	}

	expectedNormalized := "SELECT * FROM TABLE"
	if cs.normalized != expectedNormalized {
		t.Errorf("expected normalized command to be %s, got %s", expectedNormalized, cs.normalized)
	}

	expectedParts := []string{"SELECT", "*", "FROM", "TABLE"}
	for i, part := range expectedParts {
		if cs.parts[i] != part {
			t.Errorf("expected part %d to be %s, got %s", i, part, cs.parts[i])
		}
	}
}

func TestValidate(t *testing.T) {
	cs := commandFromString("SELECT * FROM table")
	err := cs.validate(4)
	if err != nil {
		t.Errorf("expected no error, got %s", err)
	}

	err = cs.validate(5)
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestValidateExact(t *testing.T) {
	cs := commandFromString("SELECT * FROM table")
	err := cs.validateExact(4)
	if err != nil {
		t.Errorf("expected no error, got %s", err)
	}

	err = cs.validateExact(3)
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestRemoveCommand(t *testing.T) {
	cs := commandFromString("SELECT * FROM table")
	newCs := cs.removeCommand(2)

	expected := "FROM table"
	if newCs.raw != expected {
		t.Errorf("expected raw command to be %s, got %s", expected, newCs.raw)
	}
}

func TestSelectCommand(t *testing.T) {
	cs := commandFromString("SELECT * FROM table")
	part := cs.selectCommand(2)

	expected := "FROM"
	if part != expected {
		t.Errorf("expected part to be %s, got %s", expected, part)
	}
}

func TestSelectNormalizedCommand(t *testing.T) {
	cs := commandFromString("SELECT * FROM table")
	part := cs.selectNormalizedCommand(2)

	expected := "FROM"
	if part != expected {
		t.Errorf("expected part to be %s, got %s", expected, part)
	}
}

func TestReplaceCommand(t *testing.T) {
	cs := commandFromString("SELECT LOCAL * FROM table")
	newCs := cs.replaceCommand("SELECT LOCAL", "SELECT")

	expected := "SELECT * FROM table"
	if newCs.raw != expected {
		t.Errorf("expected raw command to be %s, got %s", expected, newCs.raw)
	}
}
