package commands

import "testing"

func TestKey_Subcommand_Dispatch(t *testing.T) {
	cases := []struct {
		in   string
		want any
	}{
		{"KEY GET x.y", &KeyGetCommand{}},
		{"KEY PUT x.y value", &KeyPutCommand{}},
		{"KEY SET x.y value", &KeyPutCommand{}},
		{"KEY DEL x.y", &KeyDelCommand{}},
	}
	for _, c := range cases {
		cs := CommandFromString(c.in)
		cmd, err := cs.GetNext()
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", c.in, err)
		}
		// Type assertions checked explicitly below per expected type.
		if c.want != nil {
			switch c.want.(type) {
			case *KeyGetCommand:
				if _, ok := cmd.(*KeyGetCommand); !ok {
					t.Fatalf("%s: expected *KeyGetCommand, got %T", c.in, cmd)
				}
			case *KeyPutCommand:
				if _, ok := cmd.(*KeyPutCommand); !ok {
					t.Fatalf("%s: expected *KeyPutCommand, got %T", c.in, cmd)
				}
			case *KeyDelCommand:
				if _, ok := cmd.(*KeyDelCommand); !ok {
					t.Fatalf("%s: expected *KeyDelCommand, got %T", c.in, cmd)
				}
			}
		}
	}
}

func TestScan_Count_Sample_Dispatch(t *testing.T) {
	cs := CommandFromString("SCAN foo")
	cmd, err := cs.GetNext()
	if err != nil {
		t.Fatalf("SCAN: unexpected error: %v", err)
	}
	if _, ok := cmd.(*ScanCommand); !ok {
		t.Fatalf("SCAN: expected *ScanCommand, got %T", cmd)
	}

	cs = CommandFromString("COUNT foo")
	cmd, err = cs.GetNext()
	if err != nil {
		t.Fatalf("COUNT: unexpected error: %v", err)
	}
	if _, ok := cmd.(*CountCommand); !ok {
		t.Fatalf("COUNT: expected *CountCommand, got %T", cmd)
	}

	cs = CommandFromString("SAMPLE foo N 5")
	cmd, err = cs.GetNext()
	if err != nil {
		t.Fatalf("SAMPLE: unexpected error: %v", err)
	}
	if _, ok := cmd.(*SampleCommand); !ok {
		t.Fatalf("SAMPLE: expected *SampleCommand, got %T", cmd)
	}
}
