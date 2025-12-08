package streams

// Consistency test to verify each implementation returns identical results across multiple runs
// Run with: go test -run TestImplementationConsistency -v pkg/streams/*.go

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

func TestImplementationConsistency(t *testing.T) {
	streamName := "dreamz"
	filter := []string{fmt.Sprintf("core.%s.*.entries.*.>", streamName)}

	jStream, err := NewStream(natsURL, streamName)
	if err != nil {
		t.Fatal(err)
	}

	runs := 5

	implementations := map[string]func([]string) (map[string][][]byte, error){
		"v0.2.33 (FetchNoWait)":             jStream.lastPerSubject_v0233,
		"v0.3.0 (Fetch 1s timeout)":         jStream.lastPerSubject_v030,
		"v0.3.1 (NumPending + FetchNoWait)": jStream.lastPerSubject_v031,
		"Current (Fetch 2s timeout)":        jStream.lastPerSubject_current,
	}

	// Order for deterministic output
	implOrder := []string{
		"v0.2.33 (FetchNoWait)",
		"v0.3.0 (Fetch 1s timeout)",
		"v0.3.1 (NumPending + FetchNoWait)",
		"Current (Fetch 2s timeout)",
	}

	fmt.Printf("\n=== Testing Consistency Across %d Runs ===\n\n", runs)

	for _, implName := range implOrder {
		implFunc := implementations[implName]

		fmt.Printf("Testing: %s\n", implName)

		var firstTotal int
		var firstResults map[string]int
		inconsistencies := 0

		for run := 1; run <= runs; run++ {
			lmps, err := implFunc(filter)
			if err != nil {
				t.Errorf("%s Run %d failed: %v", implName, run, err)
				continue
			}

			total := 0
			entryCount := make(map[string]int)
			for subject := range lmps {
				ct := strings.Split(subject, ".")[4]
				entryCount[ct]++
				total++
			}

			if run == 1 {
				firstTotal = total
				firstResults = entryCount
				fmt.Printf("  Run 1: %d entries (baseline)\n", total)
			} else {
				if total != firstTotal {
					inconsistencies++
					fmt.Printf("  Run %d: %d entries ❌ INCONSISTENT (expected %d, diff: %+d)\n",
						run, total, firstTotal, total-firstTotal)

					// Show which content types changed
					for ct, count := range entryCount {
						if firstResults[ct] != count {
							fmt.Printf("    - %s: got %d, expected %d (diff: %+d)\n",
								ct, count, firstResults[ct], count-firstResults[ct])
						}
					}
					// Check for missing content types
					for ct, expectedCount := range firstResults {
						if entryCount[ct] == 0 {
							fmt.Printf("    - %s: MISSING (expected %d)\n", ct, expectedCount)
						}
					}
				} else {
					// Still check individual content types
					ctInconsistent := false
					for ct, count := range entryCount {
						if firstResults[ct] != count {
							if !ctInconsistent {
								fmt.Printf("  Run %d: %d entries (total matches but content types differ) ⚠️\n", run, total)
								ctInconsistent = true
								inconsistencies++
							}
							fmt.Printf("    - %s: got %d, expected %d\n", ct, count, firstResults[ct])
						}
					}
					if !ctInconsistent {
						fmt.Printf("  Run %d: %d entries ✓ Consistent\n", run, total)
					}
				}
			}
		}

		if inconsistencies == 0 {
			fmt.Printf("  ✅ PASS: All %d runs consistent\n\n", runs)
		} else {
			fmt.Printf("  ❌ FAIL: %d out of %d runs were inconsistent\n\n", inconsistencies, runs-1)
			t.Errorf("%s: Failed consistency check (%d/%d runs inconsistent)", implName, inconsistencies, runs-1)
		}
	}

	// Cross-implementation comparison
	fmt.Printf("=== Cross-Implementation Comparison ===\n\n")

	results := make(map[string]map[string]int)
	totals := make(map[string]int)

	for _, implName := range implOrder {
		implFunc := implementations[implName]
		lmps, err := implFunc(filter)
		if err != nil {
			t.Errorf("%s failed: %v", implName, err)
			continue
		}

		total := 0
		entryCount := make(map[string]int)
		for subject := range lmps {
			ct := strings.Split(subject, ".")[4]
			entryCount[ct]++
			total++
		}

		results[implName] = entryCount
		totals[implName] = total
	}

	// Compare all implementations against v0.3.0 (most reliable)
	baseline := "v0.3.0 (Fetch 1s timeout)"
	baselineResults := results[baseline]
	baselineTotal := totals[baseline]

	fmt.Printf("Baseline: %s = %d entries\n\n", baseline, baselineTotal)

	for _, implName := range implOrder {
		if implName == baseline {
			continue
		}

		total := totals[implName]
		diff := total - baselineTotal

		if total == baselineTotal {
			// Check content types
			allMatch := true
			for ct, count := range results[implName] {
				if baselineResults[ct] != count {
					allMatch = false
					break
				}
			}

			if allMatch {
				fmt.Printf("%s: ✅ Matches baseline (%d entries)\n", implName, total)
			} else {
				fmt.Printf("%s: ⚠️  Total matches but content types differ (%d entries)\n", implName, total)
			}
		} else {
			fmt.Printf("%s: ❌ Does NOT match baseline (%d entries, diff: %+d)\n", implName, total, diff)
		}
	}

	// Show all content types from baseline
	fmt.Printf("\n=== Baseline Content Types ===\n")
	contentTypes := make([]string, 0)
	for ct := range baselineResults {
		contentTypes = append(contentTypes, ct)
	}
	sort.Strings(contentTypes)
	for _, ct := range contentTypes {
		fmt.Printf("%s: %d\n", ct, baselineResults[ct])
	}
}
